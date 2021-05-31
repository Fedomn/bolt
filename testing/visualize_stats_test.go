package testing

import (
	"fmt"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// DB is a test wrapper for bolt.DB.
type DB struct {
	*bolt.DB
	Path string
}

func createDB(options *bolt.Options) *DB {
	filePath := fmt.Sprintf("../cmd/bolt/bolt-%s", time.Now().Format("2006-01-02.15:04:05"))
	_ = ioutil.WriteFile(filePath, []byte{}, 0666)

	db, err := bolt.Open(filePath, 0666, options)
	if err != nil {
		panic(err.Error())
	}
	return &DB{DB: db, Path: filePath}
}

func Test_NodeSpillStats(t *testing.T) {
	db := createDB(nil)
	_ = db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucket([]byte("test"))
		for i := 0; i < 300; i++ {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			val := strconv.Itoa(r.Int())
			if err := b.Put([]byte(val), []byte(val)); err != nil {
				return err
			}
		}
		return nil
	})
	fmt.Println("======== stats ==============")
	db.PrintStats()
	fmt.Println("======== pages ==============")
	db.pagesStats()

	fmt.Println("======== open again ==============")
	_ = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		for i := 0; i < 10; i++ {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			val := strconv.Itoa(r.Int())
			if err := b.Put([]byte(val), []byte(val)); err != nil {
				return err
			}
		}
		return nil
	})
	fmt.Println("======== stats ==============")
	db.PrintStats()
	fmt.Println("======== pages ==============")
	db.pagesStats()

	_ = db.Close()
	_ = os.Remove(db.Path)
}

// PrintStats prints the database stats
func (db *DB) PrintStats() {
	var stats = db.Stats()
	fmt.Printf("[db] %-20s %-20s %-20s\n",
		fmt.Sprintf("pg(%d/%d)", stats.TxStats.PageCount, stats.TxStats.PageAlloc),
		fmt.Sprintf("cur(%d)", stats.TxStats.CursorCount),
		fmt.Sprintf("node(%d/%d)", stats.TxStats.NodeCount, stats.TxStats.NodeDeref),
	)
	fmt.Printf("     %-20s %-20s %-20s\n",
		fmt.Sprintf("rebal(%d/%v)", stats.TxStats.Rebalance, truncDuration(stats.TxStats.RebalanceTime)),
		fmt.Sprintf("spill(%d/%v)", stats.TxStats.Spill, truncDuration(stats.TxStats.SpillTime)),
		fmt.Sprintf("w(%d/%v)", stats.TxStats.Write, truncDuration(stats.TxStats.WriteTime)),
	)
}

func (db *DB) pagesStats() {
	// Write header.
	fmt.Println("ID       TYPE       ITEMS  OVRFLW")
	fmt.Println("======== ========== ====== ======")

	_ = db.Update(func(tx *bolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				fmt.Printf("PageError: ID: %v, Err: %v", id, err)
				os.Exit(1)
			} else if p == nil {
				break
			}

			// Only display count and overflow if this is a non-free page.
			var count, overflow string
			if p.Type != "free" {
				count = strconv.Itoa(p.Count)
				if p.OverflowCount > 0 {
					overflow = strconv.Itoa(p.OverflowCount)
				}
			}

			// Print table row.
			fmt.Printf("%-8d %-10s %-6s %-6s\n", p.ID, p.Type, count, overflow)

			// Move to the next non-overflow page.
			id += 1
			if p.Type != "free" {
				id += p.OverflowCount
			}
		}
		return nil
	})
}

func truncDuration(d time.Duration) string {
	return regexp.MustCompile(`^(\d+)(\.\d+)`).ReplaceAllString(d.String(), "$1")
}
