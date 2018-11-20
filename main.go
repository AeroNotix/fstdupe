package main

import (
	"bytes"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
)

var searchDir = flag.String("dir", "/", "The directory to search for duplicated files")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var maplock = &sync.Mutex{}
var contenthashes = make(map[string][]string)
var partialcontents = make(map[string][]string)
var filesizes = make(map[int64][]string)
var filewalkers = sync.WaitGroup{}
var hashers = sync.WaitGroup{}
var initialcomparisonsize = int64(1024)

type initialComparisonJob struct {
	path string
	size int64
}

type hashJob struct {
	path string
}

func HashFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	tablePolynomial := crc32.MakeTable(0xedb88320)
	h := crc32.New(tablePolynomial)

	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	filehash := fmt.Sprintf("%x", h.Sum(nil))
	maplock.Lock()
	defer maplock.Unlock()
	contenthashes[filehash] = append(contenthashes[filehash], path)
	hashers.Done()
}

func ReadPartOfFile(path string, size int64) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	buf := bytes.NewBuffer(make([]byte, size))
	lr := io.LimitReader(f, size)
	_, err = io.Copy(buf, lr)
	if err != nil {
		fmt.Println(err)
	}
	s := buf.String()
	maplock.Lock()
	defer maplock.Unlock()
	partialcontents[s] = append(partialcontents[s], path)
	hashers.Done()
}

func doFindDuplicateFileSizes(root string) error {
	filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if root == path {
			return nil
		}
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			return nil
		}
		if f.Size() == 0 {
			return nil
		}
		if f.IsDir() {
			filewalkers.Add(1)
			go doFindDuplicateFileSizes(path)
			return filepath.SkipDir
		}
		maplock.Lock()
		defer maplock.Unlock()
		filesizes[f.Size()] = append(filesizes[f.Size()], path)
		return nil
	})
	filewalkers.Done()
	return nil
}

func FindDuplicatesInPath(root string) {
	filewalkers.Add(1)
	doFindDuplicateFileSizes(root)
	filewalkers.Wait()
	initialComparisons := make(chan initialComparisonJob)
	hashes := make(chan hashJob)
	for x := 0; x < runtime.NumCPU(); x++ {
		go func() {
			for job := range initialComparisons {
				ReadPartOfFile(job.path, job.size)
			}
		}()
		go func() {
			for job := range hashes {
				HashFile(job.path)
			}
		}()
	}
	for _, paths := range filesizes {
		if len(paths) > 1 {
			for _, path := range paths {
				hashers.Add(1)
				initialComparisons <- initialComparisonJob{path, 4096}
			}
		}
	}
	hashers.Wait()
	for _, paths := range partialcontents {
		if len(paths) > 1 {
			for _, path := range paths {
				hashers.Add(1)
				hashes <- hashJob{path}
			}
		}
	}
	hashers.Wait()
}

func ReportDuplicates() {
	for _, paths := range contenthashes {
		f, err := os.Open(paths[0])
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size := fi.Size()
		fmt.Printf("Potential savings of %d bytes in duplicated files:\n", size*(int64(len(paths)-1)))
		for _, path := range paths {
			fmt.Printf("\t%s\n", path)
		}
	}
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	fmt.Println(*searchDir)
	FindDuplicatesInPath(*searchDir)
	ReportDuplicates()
}
