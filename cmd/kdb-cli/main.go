// kdb-cli is a small interactive REPL for the kdb key/value store.
//
//	$ kdb-cli -dir ./data
//	> put hello world
//	OK
//	> get hello
//	"world"
//	> stats
//	memtable: 1 entries, 10 bytes
//	sstables: 0
//	> quit
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/owolagbadavid/kdb"
)

func main() {
	dir := flag.String("dir", "./kdbdata", "directory holding the kdb data files")
	flag.Parse()

	db, err := kdb.Open(*dir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", *dir, err)
		os.Exit(1)
	}

	// Ensure the WAL is closed cleanly on SIGINT / SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\n(interrupted, closing)")
		_ = db.Close()
		os.Exit(0)
	}()

	fmt.Fprintf(os.Stderr, "kdb-cli on %s — type 'help' for commands\n", *dir)
	if err := repl(db, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "repl: %v\n", err)
	}
	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		os.Exit(1)
	}
}

func repl(db *kdb.DB, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for {
		fmt.Fprint(os.Stderr, "> ")
		if !scanner.Scan() {
			fmt.Fprintln(os.Stderr) // newline after Ctrl-D
			return scanner.Err()
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		cmd := strings.ToLower(fields[0])
		args := fields[1:]
		switch cmd {
		case "put":
			if len(args) != 2 {
				fmt.Fprintln(out, "usage: put <key> <value>")
				continue
			}
			if err := db.Put([]byte(args[0]), []byte(args[1])); err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			fmt.Fprintln(out, "OK")
		case "get":
			if len(args) != 1 {
				fmt.Fprintln(out, "usage: get <key>")
				continue
			}
			v, err := db.Get([]byte(args[0]))
			if errors.Is(err, kdb.ErrNotFound) {
				fmt.Fprintln(out, "(nil)")
				continue
			}
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			fmt.Fprintf(out, "%q\n", v)
		case "delete", "del":
			if len(args) != 1 {
				fmt.Fprintln(out, "usage: delete <key>")
				continue
			}
			if err := db.Delete([]byte(args[0])); err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			fmt.Fprintln(out, "OK")
		case "stats":
			s := db.Stats()
			fmt.Fprintf(out, "memtable: %d entries, %d bytes\n", s.MemtableCount, s.MemtableSizeBytes)
			if len(s.SSTables) == 0 {
				fmt.Fprintln(out, "sstables: 0")
			} else {
				names := make([]string, 0, len(s.SSTables))
				for _, t := range s.SSTables {
					names = append(names, fmt.Sprintf("%06d.sst@T%d", t.FileNum, t.Tier))
				}
				fmt.Fprintf(out, "sstables: %d (%s)\n", len(s.SSTables), strings.Join(names, ", "))
			}
		case "help":
			fmt.Fprintln(out, "commands:")
			fmt.Fprintln(out, "  put <key> <value>     store a value")
			fmt.Fprintln(out, "  get <key>             read a value")
			fmt.Fprintln(out, "  delete <key>          delete a key")
			fmt.Fprintln(out, "  stats                 show memtable size and SSTable list")
			fmt.Fprintln(out, "  help                  this message")
			fmt.Fprintln(out, "  quit / exit / Ctrl-D  close and exit")
		case "quit", "exit":
			return nil
		default:
			fmt.Fprintf(out, "unknown command %q (try 'help')\n", cmd)
		}
	}
}
