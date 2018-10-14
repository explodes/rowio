package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/explodes/cli"
	"github.com/explodes/rowio"
	"github.com/explodes/rowio/cmd/cli/protos"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
)

const (
	requestTimeout = 20 * time.Second
	defaultHost    = "0.0.0.0:8234"
	defaultBucket  = "main"
)

var (
	mainMenu      = []string{"connect", "connect default", "exit"}
	connectedMenu = []string{"set bucket", "disconnect", "exit"}
	bucketSetMenu = []string{"set bucket", "add user", "list bucket", "disconnect", "exit"}
)

func main() {
	app := &App{}
	defer func() {
		if app.conn != nil {
			app.conn.Close()
		}
	}()

	app.loop()

}

type App struct {
	conn      *grpc.ClientConn
	client    rowio.RowIOServiceClient
	connected string
	bucket    string
}

func (app *App) handleAction(action string) {
	switch action {
	case "exit":
		app.disconnect()
		os.Exit(0)
	case "connect":
		app.connect()
	case "connect default":
		app.connectDefault()
	case "disconnect":
		app.disconnect()
	case "set bucket":
		app.setBucket()
	case "list bucket":
		app.listBucket()
	case "add user":
		app.addUser()
	default:
		fmt.Println("unknown selection")
	}
	app.loop()
}

func (app *App) disconnect() {
	if app.conn == nil {
		return
	}
	err := app.conn.Close()
	if err != nil {
		log.Printf("error with close: %v", err)
	}
	fmt.Printf("disconnected from %s\n", app.connected)
	app.connected = ""
	app.bucket = ""
	app.conn = nil
	app.client = nil
}

func (app *App) connect() {
	app.disconnect()
	host := cli.PromptNonEmptyString("host> ")
	app.connectTo(host)
}

func (app *App) connectDefault() {
	app.connectTo(defaultHost)
	app.bucket = defaultBucket
}

func (app *App) connectTo(host string) {
	conn, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("error connecting: %v", err)
		return
	}
	app.conn = conn
	app.connected = host
	app.client = rowio.NewRowIOServiceClient(conn)
}

func (app *App) loop() {
	app.printStatus()

	var menu []string

	switch {
	case app.bucket != "":
		menu = bucketSetMenu
	case app.connected != "":
		menu = connectedMenu
	default:
		menu = mainMenu
	}
	selection := cli.PresentMenu("?> ", menu...)
	app.handleAction(menu[selection])
}

func (app *App) printStatus() {
	if app.connected != "" {
		fmt.Printf("CONNECTED TO HOST: %s\n", app.connected)
	}
	if app.bucket != "" {
		fmt.Printf("BUCKET: %s\n", app.bucket)
	}
}

func (app *App) setBucket() {
	app.bucket = cli.PromptNonEmptyString("name> ")
}

func (app *App) listBucket() {
	request := &rowio.ScanRequest{
		Bucket:  app.bucket,
		FromKey: int64bytes(0),
		ToKey:   int64bytes(math.MaxInt64),
	}
	stream, err := app.client.Scan(requestContext(), request)
	if err != nil {
		log.Printf("unable to get users: %v", err)
	}
	for {
		val, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Printf("scan error: %v", err)
			}
			break
		}
		fmt.Printf("any: %s\n", val)
		user := &protos.User{}
		ptypes.UnmarshalAny(val.Value, user)
		fmt.Printf("user: %s\n", user)
	}
}

func (app *App) addUser() {
	username := cli.PromptNonEmptyString("username> ")
	created := time.Now().Unix()
	user := &protos.User{
		Username: username,
		Created:  created,
	}
	value, err := ptypes.MarshalAny(user)
	if err != nil {
		log.Printf("unable to marshal user: %v", err)
		return
	}
	request := &rowio.SetRequest{
		Bucket: app.bucket,
		Key:    int64bytes(created),
		Value:  value,
	}
	_, err = app.client.Set(requestContext(), request)
	if err != nil {
		log.Printf("error saving user: %v", err)
		return
	}
}

func requestContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	return ctx
}

func int64bytes(i int64) []byte {
	b := make([]byte, 16)
	binary.PutVarint(b, i)
	return b
}
