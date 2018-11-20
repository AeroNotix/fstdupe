package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/cespare/xxhash"
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
var contenthashes = make(map[uint64][]string, 50000)
var partialcontents = make(map[string][]string, 50000)
var filesizes = make(map[int64][]string, 50000)
var filewalkers = sync.WaitGroup{}
var hashers = sync.WaitGroup{}
var initialcomparisonsize = int64(1024)

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, initialcomparisonsize))
	},
}

func HashFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := xxhash.New()

	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	maplock.Lock()
	defer maplock.Unlock()
	filehash := h.Sum64()
	contenthashes[filehash] = append(contenthashes[filehash], path)
	hashers.Done()
}

func ReadPartOfFile(path string) {
	defer hashers.Done()
	f, err := os.Open(path)
	if err != nil {
		if os.IsPermission(err) {
			return
		}
		log.Fatal(err)
	}
	defer f.Close()

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	lr := io.LimitReader(f, initialcomparisonsize)
	_, err = io.Copy(buf, lr)
	if err != nil {
		fmt.Println(err)
	}
	s := buf.String()
	maplock.Lock()
	defer maplock.Unlock()
	partialcontents[s] = append(partialcontents[s], path)
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
	initialComparisons := make(chan string)
	hashes := make(chan string)
	for x := 0; x < runtime.NumCPU()*4; x++ {
		bufPool.Get()
		go func() {
			for path := range initialComparisons {
				ReadPartOfFile(path)
			}
		}()
		go func() {
			for path := range hashes {
				HashFile(path)
			}
		}()
	}
	for _, paths := range filesizes {
		if len(paths) > 1 {
			for _, path := range paths {
				hashers.Add(1)
				initialComparisons <- path
			}
		}
	}
	hashers.Wait()
	for _, paths := range partialcontents {
		if len(paths) > 1 {
			for _, path := range paths {
				hashers.Add(1)
				hashes <- path
			}
		}
	}
	hashers.Wait()
}

func ReportDuplicates() {
	for _, paths := range contenthashes {
		if len(paths) > 1 {
			f, err := os.Open(paths[0])
			if err != nil {
				if err == os.ErrPermission {
					break
				}
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
}

func ReportDuplicatesSimple() {
	for _, paths := range contenthashes {
		if len(paths) > 1 {
			fmt.Println(paths)
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
	ReportDuplicatesSimple()
}
