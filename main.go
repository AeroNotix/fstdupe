package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/minio/highwayhash"
	"golang.org/x/crypto/md4" // don't at me with "insecure crypto" shite
	"hash"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
)

var searchDir = flag.String("dir", "/", "The directory to search for duplicated files")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var maplock = &sync.Mutex{}
var contenthashes = make(map[string][]string)
var partialcontenthashes = make(map[string][]string)
var filesizes = make(map[int64][]string)
var filewalkers = sync.WaitGroup{}
var hashers = sync.WaitGroup{}
var initialcomparisonsize = int64(1024)

// don't care about cryptographically secure hashes
var key, err = hex.DecodeString("0000000000000000000000000000000000000000000000000000000000000000")

func usingshite() {
	highwayhash.New(key)
	md4.New()
	tablePolynomial := crc32.MakeTable(0xedb88320)
	crc32.New(tablePolynomial)
}

func getHasher() hash.Hash {
	// h, _ := highwayhash.New(key)
	// return h

	tablePolynomial := crc32.MakeTable(0xedb88320)
	h := crc32.New(tablePolynomial)
	return h
}

func HashFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := getHasher()

	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	filehash := fmt.Sprintf("%x", h.Sum(nil))
	maplock.Lock()
	defer maplock.Unlock()
	contenthashes[filehash] = append(contenthashes[filehash], path)
	hashers.Done()
}

func HashPartOfFile(path string, size int64) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := getHasher()

	lr := io.LimitReader(f, size)
	if _, err := io.Copy(h, lr); err != nil {
		log.Fatal(err)
	}
	filehash := fmt.Sprintf("%x", h.Sum(nil))
	maplock.Lock()
	defer maplock.Unlock()
	partialcontenthashes[filehash] = append(partialcontenthashes[filehash], path)
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
	partialcontenthashes[s] = append(partialcontenthashes[s], path)
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
	inflight := 0
	for _, paths := range filesizes {
		if len(paths) > 1 {
			for _, path := range paths {
				hashers.Add(1)
				inflight++
				go ReadPartOfFile(path, initialcomparisonsize)
				if inflight == 8 {
					hashers.Wait()
					inflight = 0
				}
			}
		}
	}
	hashers.Wait()
	for _, paths := range partialcontenthashes {
		if len(paths) > 1 {
			for _, path := range paths {
				hashers.Add(1)
				inflight++
				go HashFile(path)
				if inflight == 8 {
					hashers.Wait()
					inflight = 0
				}
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
