package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/urfave/cli/v2"
	"log"
	"math/big"
	"os"
	"strconv"
	"time"
)

type entry struct {
	BlockTime  uint64
	L1Root     []byte
	ParentHash []byte
}

func (e *entry) Marshal() []byte {
	result := []byte{}
	result = append(result, e.L1Root...)
	result = append(result, e.ParentHash...)
	timestamp := make([]byte, 8)
	binary.Encode(timestamp, binary.NativeEndian, e.BlockTime)
	result = append(result, timestamp...)
	return result
}

func (e *entry) Unmarshal(data []byte) {
	e.L1Root = data[:32]
	e.ParentHash = data[32:64]
	blkTime := data[64 : 64+8]
	binary.Decode(blkTime, binary.NativeEndian, &e.BlockTime)
}

func main() {
	app := cli.App{
		Name:  "Gateway.fm Assignment",
		Usage: "Index certain Sepolia contract's events",
		Authors: []*cli.Author{
			{
				Name:  "Sviatoslav Osin",
				Email: "svosin@gmail.com",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "run",
				Aliases: []string{"r"},
				Usage:   "Scrape the blockchain",
				Action:  start,
			},
			{
				Name:    "dump",
				Aliases: []string{"d"},
				Usage:   "Print collected events",
				Action:  dump,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func start(c *cli.Context) error {
	initEthclient()

	db, err := leveldb.OpenFile("events.ldb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	account := common.HexToAddress("0xA13Ddb14437A8F34897131367ad3ca78416d6bCa")

	currentBlockNumber, err := Client.BlockNumber(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// split the workload into chunks that can be digested by the API

	// send them and build the event database
	var startBlock *big.Int
	starterBlockArg := c.Args().First()
	if starterBlockArg == "" {
		startBlock = big.NewInt(int64(currentBlockNumber - 3000))
	} else {
		bn, _ := strconv.ParseUint(starterBlockArg, 10, 64)
		startBlock = big.NewInt(int64(bn))
	}

	topic := common.BytesToHash(common.FromHex("0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766"))
	fmt.Println("Topic:", topic.String())

	query := ethereum.FilterQuery{
		FromBlock: startBlock, //big.NewInt(6523000),
		//ToBlock:   big.NewInt(3811),
		Addresses: []common.Address{account},
		Topics:    [][]common.Hash{{topic}},
	}

	logs, err := Client.FilterLogs(context.Background(), query)
	if err != nil {
		log.Fatal(err)
	}

	var counter uint64

	for _, l := range logs {
		fmt.Println("Log Data:", l.Index, l.BlockNumber, common.Bytes2Hex(l.Data))
		block, err := Client.HeaderByNumber(context.Background(), big.NewInt(int64(l.BlockNumber)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("PH, Time:", block.ParentHash, block.Time)
		e := entry{
			L1Root:     l.Data,
			BlockTime:  block.Time,
			ParentHash: block.ParentHash.Bytes(),
		}
		ctr := make([]byte, 8)
		_, err = binary.Encode(ctr, binary.NativeEndian, counter)
		if err != nil {
			log.Fatal(err)
		}
		err = db.Put(ctr, e.Marshal(), nil)
		if err != nil {
			log.Fatal(err)
		}
		counter++
	}
	fmt.Printf("processed %d entries\n", counter)
	return nil
}

func dump(c *cli.Context) error {
	db, err := leveldb.OpenFile("events.ldb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		idx := iter.Key()

		index := binary.NativeEndian.Uint64(idx)

		var currEntry entry
		currEntry.Unmarshal(iter.Value())
		log.Println(index, time.Unix(int64(currEntry.BlockTime), 0), common.Bytes2Hex(currEntry.L1Root), common.Bytes2Hex(currEntry.ParentHash))
	}
	return nil
}
