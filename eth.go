package main

import (
	"github.com/ethereum/go-ethereum/ethclient"
	"log"
)

var Client *ethclient.Client

func initEthclient() {
	client, err := ethclient.Dial("https://rpc.ankr.com/eth_sepolia")
	if err != nil {
		log.Fatal(err)
	}
	Client = client
}
