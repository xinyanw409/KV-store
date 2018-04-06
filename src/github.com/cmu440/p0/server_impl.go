/* Author: xinyanw.
 * Lab description: In this lab I implemented a KeyValueServer which can accept multiple
 * clients and return response to corresponding client. I used channels to avoid race
 * condition. In my code, I define two structs: server and client. In server struct there
 * is a map to store all the client connected to server. Each client in my code has a read
 * and write routine. When error happens during read, the client will be disconnected from
 * server and it will be deleted from map. When Close() function called, all cliets will be
 * closed.
 */

package p0

import (
	"bufio"
	"net"
	"strconv"
)

// Struct of server.
type keyValueServer struct {
	listen           net.Listener
	clienter         map[int]*Clienter // Map to store clients
	connectedClients int               // Last id of client in map
	connChannel      chan net.Conn     // Channel control add event
	close_signal     chan bool         // Channel control close event
	cnt_signal_in    chan bool         // Channel control count event
	cnt_signal_out   chan int          // Channel return counter of clients
	delete           chan *Clienter    // Channel control deletion
	req              chan *Request     // Channel pass request from clients
	request          *Request
}

// Struct of request, including request string and client id.
type Request struct {
	input string
	cid   int
}

// Struct of client.
type Clienter struct {
	cid        int
	connection net.Conn
	kvs        *keyValueServer
	response   chan string // Channel return response from database
	reader     *bufio.Reader
	stopWriter chan bool // Channel pass signal to stop write routine
}

// New creates and returns (but does not start) a new KeyValueServer. Initial a database.
func New() KeyValueServer {
	init_db()
	kvs := &keyValueServer{
		clienter:         make(map[int]*Clienter),
		connectedClients: -1,
		connChannel:      make(chan net.Conn),
		close_signal:     make(chan bool),
		cnt_signal_in:    make(chan bool),
		cnt_signal_out:   make(chan int),
		delete:           make(chan *Clienter),
		req:              make(chan *Request),
	}
	return kvs
}

// Start server listener, use goroutine to accept client connection.
func (kvs *keyValueServer) Start(port int) error {
	str := ":"
	str += strconv.Itoa(port)
	listen, err := net.Listen("tcp", str)
	if err != nil {
		return err
	}
	kvs.listen = listen
	go kvs.mainRoutine()

	go func() {
		for {
			conn, err := listen.Accept()
			if err != nil {
				continue
			}
			kvs.connChannel <- conn
		}
	}()
	return nil
}

// When Close() function called, send a signal to make mainroutine choose.
// close event
func (kvs *keyValueServer) Close() {
	kvs.close_signal <- true
}

// Function to return the number of clients connected to server.
func (kvs *keyValueServer) Count() int {
	kvs.cnt_signal_in <- true
	cnt := <-kvs.cnt_signal_out
	return cnt
}

// TODO: add additional methods/functions below!
// Create a new client with its id, server and connection.
func newClienter(cid int, kvs *keyValueServer, connection net.Conn) *Clienter {
	reader := bufio.NewReader(connection)

	client := &Clienter{
		cid:        cid,
		connection: connection,
		kvs:        kvs,
		response:   make(chan string, 500),
		reader:     reader,
		stopWriter: make(chan bool),
	}
	return client
}

// Function to add a new client to map and create a new read and write routine
// to this client.
func (kvs *keyValueServer) handleConnection(connection net.Conn) {
	newClientId := kvs.connectedClients + 1
	kvs.connectedClients = newClientId

	client := newClienter(newClientId, kvs, connection)
	go client.Read()
	go client.Write()
	// If the client already existed in the map, ignore it; otherwise add the client
	// to map.
	_, ok := kvs.clienter[newClientId]
	if !ok {
		kvs.clienter[newClientId] = client
	}
}

// Main routine with five event selection: add to map, handle request from clients,
// close all clients and server, return counter of clients, delete from map.
func (kvs *keyValueServer) mainRoutine() {
	for {
		select {
		// Case to handle request from clients.
		case data := <-kvs.req:
			kvs.handleRequest(data)
		// Case to add a new client to map.
		case conn := <-kvs.connChannel:
			kvs.handleConnection(conn)
		// Case to close all the clients and server.
		case <-kvs.close_signal:
			for _, v := range kvs.clienter {
				v.connection.Close()
				v.stopWriter <- true
			}
			kvs.listen.Close()
			return
		// Case to delete client from map.
		case c := <-kvs.delete:
			c.stopWriter <- true
			delete(kvs.clienter, c.cid)
		// Case to return counter.
		case <-kvs.cnt_signal_in:
			cnt := len(kvs.clienter)
			kvs.cnt_signal_out <- cnt
		}
	}
}

// Function to handle put and get request from clients.
func (kvs *keyValueServer) handleRequest(req *Request) {
	var request []string
	request = kvs.parseRequest(req.input)
	if request[0] == "get" {
		client := kvs.clienter[req.cid]
		kvs.getFromDB(request, client)
	}
	if request[0] == "put" {
		kvs.putIntoDB(request)
	}
}

// Read routine to read request from client. Each client has its own
// read routine.
func (client *Clienter) Read() {
	for {
		message, err := client.reader.ReadString('\n')
		// If read error, send a signal to delete the client and drop the routine.
		if err != nil {
			client.connection.Close()
			client.kvs.delete <- client
			break
		}
		id := client.cid
		request := &Request{
			input: message,
			cid:   id,
		}
		client.kvs.req <- request
	}
}

// Write routine to write response from database to client.
func (client *Clienter) Write() {
	for {
		select {
		case <-client.stopWriter:
			return
		case data := <-client.response:
			client.connection.Write([]byte(data))
		}
	}
}

// Parse function to parse the request into method, key and value.
func (kvs *keyValueServer) parseRequest(message string) []string {
	n := len(message)
	a := make([]string, 3)
	b := []byte(message)
	index := 0
	start := 0
	for i := 0; i < n-1; i++ {
		if b[i] != ',' {
			continue
		}
		a[index] = string(b[start:i])
		index++
		start = i + 1
	}
	a[index] = string(b[start : n-1])
	return a
}

// Function to put key, value to database.
func (kvs *keyValueServer) putIntoDB(request []string) {
	key := request[1]
	value := []byte(request[2])
	put(key, value)
}

// Function to get all value corresponding to key and return as a "<key>,<value>\n"
// style.
func (kvs *keyValueServer) getFromDB(request []string, client *Clienter) {
	key := request[1]
	ans := get(key)
	n := len(ans)
	for i := 0; i < n; i++ {
		res := key + "," + string(ans[i]) + "\n"
		// If response number exceed 500, return.
		select {
		case client.response <- res:
		default:
			return
		}
	}
}
