package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	ros "github.com/fukurin00/go_ros_msg"

	"github.com/golang/protobuf/proto"
	gosocketio "github.com/mtfelian/golang-socketio"
	fleet "github.com/synerex/proto_fleet"
	geo "github.com/synerex/proto_geography"
	mqtt "github.com/synerex/proto_mqtt"
	pagent "github.com/synerex/proto_people_agent"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

// Harmoware Vis-Synerex wiht Layer extension provider provides map information to Web Service through socket.io.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	assetDir        = flag.String("assetdir", "", "set Web client dir")
	mapbox          = flag.String("mapbox", "", "Set Mapbox access token")
	port            = flag.Int("port", 10080, "HarmoVis Ext Provider Listening Port")
	notUnity        = flag.Bool("noUnity", false, "do not use unity")
	mu              = new(sync.Mutex)
	version         = "0.03"
	assetsDir       http.FileSystem
	ioserv          *gosocketio.Server
	sxServerAddress string
	mapboxToken     string

	colormap       map[int][4]uint8
	eventTimestamp map[int]time.Time
	startTime      time.Time
)

// const (
// 	latBase = 35.181453  //
// 	lonBase = 136.906428 //
// 	xscale  = 9.109
// 	yscale  = 11.094
// )

//136.90620 から 136.90622
//26m
//0.00002/26
const (
	//latBase = 35.181553  //
	latBase = 35.155439749049236
	lonBase = 136.9658124300913

	//lonBase = 136.906128 //
	// xscale = 9.109
	// yscale = 11.000
	xscale = 7.6
	yscale = 7.6
)

func init() {
	eventTimestamp = make(map[int]time.Time)
	colormap = make(map[int][4]uint8)
	colormap[1] = [4]uint8{254, 0, 0, 255}
	colormap[2] = [4]uint8{0, 255, 0, 255}
	colormap[3] = [4]uint8{0, 0, 255, 255}
	colormap[4] = [4]uint8{0, 255, 255, 255}
	startTime = time.Now()
}

// assetsFileHandler for static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	file := r.URL.Path
	//	log.Printf("Open File '%s'",file)
	if file == "/" {
		file = "/index.html"
	}
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}

func run_server() *gosocketio.Server {

	currentRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	if *assetDir != "" {
		currentRoot = *assetDir
	}

	d := filepath.Join(currentRoot, "mclient", "build")

	assetsDir = http.Dir(d)
	log.Println("AssetDir:", assetsDir)

	assetsDir = http.Dir(d)
	server := gosocketio.NewServer()

	server.On(gosocketio.OnConnection, func(c *gosocketio.Channel) {
		// wait for a few milli seconds.
		log.Printf("Connected from %s as %s", c.IP(), c.Id())

	})

	server.On("get_mapbox_token", func(c *gosocketio.Channel) {
		log.Printf("Requested mapbox access token")
		mapboxToken = os.Getenv("MAPBOX_ACCESS_TOKEN")
		if *mapbox != "" {
			mapboxToken = *mapbox
		}
		c.Emit("mapbox_token", mapboxToken)
		log.Printf("mapbox-token transferred %s ", mapboxToken)
	})

	server.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel) {
		log.Printf("Disconnected from %s as %s", c.IP(), c.Id())
	})

	return server
}

type MapMarker struct {
	mtype int32   `json:"mtype"`
	id    int32   `json:"id"`
	lat   float64 `json:"lat"`
	lon   float64 `json:"lon"`
	angle float32 `json:"angle"`
	speed int32   `json:"speed"`
	ts    int64   `json:"ts"`
	ms    int     `json:"tsnano"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type Orientation struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
	W float64 `json:"w"`
}

type Pose struct {
	Header struct {
		Seq   int `json:"seq"`
		Stamp struct {
			Secs  int `json:"secs"`
			Nsecs int `json:"nsecs"`
		} `json:"stamp"`
		FrameID string `json:"frame_id"`
	} `json:"header"`
	Pose struct {
		Pos Position    `json:"position"`
		Ori Orientation `json:"orientation"`
	} `json:"pose"`
}

type HumanPose struct {
	Header struct {
		Seq   int `json:"seq"`
		Stamp struct {
			Secs  int `json:"secs"`
			Nsecs int `json:"nsecs"`
		} `json:"stamp"`
		FrameID int `json:"frame_id"`
	} `json:"header"`
	Pose struct {
		Pos Position    `json:"position"`
		Ori Orientation `json:"orientation"`
	} `json:"pose"`
}

var (
	eventTimeStamp  int64     = 0
	eventPoint      []float64 = []float64{0, 0}
	agent1TimeStamp int64     = 0
	agent1Point     []float64 = []float64{0, 0}
	agent2TimeStamp int64     = 0
	agent2Point     []float64 = []float64{0, 0}
)

func (m *MapMarker) GetJson() string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d,\"ts\":%d.%03d}",
		m.mtype, m.id, m.lat, m.lon, m.angle, m.speed, m.ts, m.ms)
	return s
}
func (m *MapMarker) GetJsonTime() string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d,\"ts\":%s}",
		m.mtype, m.id, m.lat, m.lon, m.angle, m.speed, time.Unix(m.ts, int64(m.ms*1000000)).Format(time.RFC3339))
	return s
}

func supplyRideCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	flt := &fleet.Fleet{}
	err := proto.Unmarshal(sp.Cdata.Entity, flt)
	if err == nil {
		mm := &MapMarker{
			mtype: int32(0),
			id:    flt.VehicleId,
			lat:   float64(flt.Coord.Lat),
			lon:   float64(flt.Coord.Lon),
			angle: flt.Angle,
			speed: flt.Speed,
			ts:    sp.Ts.AsTime().Unix(),                 // unix seconds
			ms:    sp.Ts.AsTime().Nanosecond() / 1000000, // Milliseconds
		}
		//		jsondata, err := json.Marshal(*mm)
		//		fmt.Println("rcb",mm.GetJson())
		mu.Lock()
		ioserv.BroadcastToAll("event", mm.GetJson())
		mu.Unlock()
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock() // first make client into nil
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeRideSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyRideCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)
	}
}

func supplyGeoCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	switch sp.SupplyName {
	case "GeoJson":
		geo := &geo.Geo{}
		err := proto.Unmarshal(sp.Cdata.Entity, geo)
		if err == nil {
			strjs := string(geo.Data)
			log.Printf("Obtaining %s, id:%d, %s, len:%d ", geo.Type, geo.Id, geo.Label, len(strjs))
			log.Printf("Data '%s'", strjs)
			mu.Lock()
			ioserv.BroadcastToAll("geojson", strjs)
			mu.Unlock()
		}
	case "Lines":
		geo := &geo.Lines{}
		err := proto.Unmarshal(sp.Cdata.Entity, geo)
		if err == nil {

			jsonBytes, _ := json.Marshal(geo.Lines)
			//			log.Printf("Lines: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("lines", string(jsonBytes))
			mu.Unlock()
		}
	case "ViewState":
		vs := &geo.ViewState{}
		err := proto.Unmarshal(sp.Cdata.Entity, vs)
		if err == nil {
			jsonBytes, _ := json.Marshal(vs)
			log.Printf("ViewState: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("mapbox_token", mapboxToken)

			ioserv.BroadcastToAll("viewstate", string(jsonBytes))
			mu.Unlock()
		}
	case "ClearMoves":
		cms := &geo.ClearMoves{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			log.Printf("ClearMoves: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("clearMoves", string(jsonBytes))
			mu.Unlock()
		}
	case "Pitch":
		cms := &geo.Pitch{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			log.Printf("Pitch: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("pitch", string(jsonBytes))
			mu.Unlock()
		}
	case "Bearing":
		cms := &geo.Bearing{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			log.Printf("Bearing: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("bearing", string(jsonBytes))
			mu.Unlock()
		}

	case "Arcs":
		cms := &geo.Arcs{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("Arcs: %v", string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToAll("arcs", string(jsonBytes))
			mu.Unlock()
		}

	case "ClearArcs":
		log.Printf("clearArc!")
		mu.Lock()
		ioserv.BroadcastToAll("clearArcs", string(0))
		mu.Unlock()

	case "Scatters":
		cms := &geo.Scatters{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("Scatters: %v", string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToAll("scatters", string(jsonBytes))
			mu.Unlock()
		}

	case "ClearScatters":
		log.Printf("clearScatter!")
		mu.Lock()
		ioserv.BroadcastToAll("clearScatters", string(0))
		mu.Unlock()

	case "TopTextLabel":
		//		log.Printf("labelInfo!")
		cms := &geo.TopTextLabel{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {

			jsonBytes, _ := json.Marshal(cms)
			//			log.Printf("LabelInfo: %v", string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToAll("topLabelInfo", string(jsonBytes))
			mu.Unlock()

		}

	case "HarmoVIS":
		cms := &geo.HarmoVIS{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			mu.Lock()
			ioserv.BroadcastToAll("harmovis", string(jsonBytes))
			mu.Unlock()

		}
	}

}

func subscribeGeoSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyGeoCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)

	}
}

func subscribeMQTTSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {
		err := client.SubscribeSupply(ctx, supplyMqttCallback)
		log.Printf("Error:Suply %s\n", err.Error())
		// we need torestart
		reconnectClient(client)
	}
}

func supplyPAgentCallback(cl *sxutil.SXServiceClient, sp *api.Supply) {
	switch sp.SupplyName {
	case "Agents":
		agents := &pagent.PAgents{}
		err := proto.Unmarshal(sp.Cdata.Entity, agents)
		if err == nil {
			seconds := sp.Ts.GetSeconds()
			nanos := sp.Ts.GetNanos()
			jsonBytes, _ := json.Marshal(agents)
			jstr := fmt.Sprintf("{ \"ts\": %d.%03d, \"dt\": %s}", seconds, int(nanos/1000000), string(jsonBytes))
			//				log.Printf("Lines: %v", jstr)
			mu.Lock()
			ioserv.BroadcastToAll("agents", jstr)
			mu.Unlock()
		}
	}

}

func subscribePAgentSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyPAgentCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)
	}
}

type TripsOpt struct {
	Path  [][2]float64 `json:"path"`
	Ts    []int64      `json:"ts"`
	Color [3]uint8     `json:"color"`
}

type PathOpt struct {
	ID    int          `json:"id"`
	Data  [][3]float64 `json:"data"`
	Color [4]uint8     `json:"color"`
}

func supplyMqttCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	//	log.Printf("Agent: %v", *sp)
	var rcd = mqtt.MQTTRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, &rcd)
	if err != nil {
		log.Printf("Erooro: proto %s\n", err.Error())
	} else {
		seconds := sp.Ts.GetSeconds()
		nanos := sp.Ts.GetNanos()
		if strings.HasPrefix(rcd.Topic, "robot/path") {
			var path ros.Path
			var id int
			fmt.Sscanf(rcd.Topic, "robot/path/%d", &id)
			err := json.Unmarshal(rcd.Record, &path)
			if err != nil {
				log.Print(err)
			}
			var pathOpt PathOpt
			for _, pos := range path.Poses {
				lat := float64(latBase + 0.0001*(pos.Pose.Position.X/xscale))
				lon := float64(lonBase + 0.0001*(pos.Pose.Position.Y/yscale))
				ts := time.Unix(int64(pos.Header.Stamp.Secs), int64(pos.Header.Stamp.Nsecs)).Sub(startTime).Seconds()
				pathOpt.Data = append(pathOpt.Data, [3]float64{lat, lon, ts})
				pathOpt.ID = id
			}
			if val, ok := colormap[id]; ok {
				pathOpt.Color = val
			} else {
				pathOpt.Color = [4]uint8{255, 255, 0, 255}
			}
			jsonBytes, err := json.Marshal(pathOpt)
			if err != nil {
				log.Print(err)
			}
			//jstr := fmt.Sprintf("{ \"ts\": %d.%03d, \"dt\": %s}", seconds, int(nanos/1000000), string(jsonBytes))
			mu.Lock()
			ioserv.BroadcastToAll("path", jsonBytes)
			mu.Unlock()
		} else if strings.HasPrefix(rcd.Topic, "robot/position") {
			var id int
			n, nerr := fmt.Sscanf(rcd.Topic, "robot/position/%d", &id)
			if _, ok := eventTimestamp[id]; !ok {
				eventTimestamp[id] = time.Unix(0, 0)
			}
			if time.Since(eventTimestamp[id]).Seconds() > 0.3 {
				if n == 1 && nerr == nil { // robot pose into location
					var pose ros.Pose
					jerr := json.Unmarshal(rcd.Record, &pose)

					if jerr == nil {
						var angle float32 = float32(2.0 * math.Acos(pose.Orientation.W))
						var lat, lon float64
						lat = float64(latBase + 0.0001*(pose.Position.X/xscale))
						lon = float64(lonBase - 0.0001*(pose.Position.Y/yscale))
						//log.Printf("%d: (%f,%f)", id, pose.Position.X, pose.Position.Y)

						mm := &MapMarker{
							mtype: int32(0),
							id:    int32(id),
							lat:   lat,
							lon:   lon,
							angle: angle,
							speed: 1,
							ts:    seconds,
							ms:    int(nanos),
						}

						log.Printf("Map:%s", mm.GetJson())
						mu.Lock()
						ioserv.BroadcastToAll("event", mm.GetJson())
						mu.Unlock()
						eventTimestamp[id] = time.Now()
					} else {
						log.Printf("Unmarshal MQTT robot record error! %v %v", err, rcd)
					}
				}
			}
		}
	}
}

func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(runtime.NumGoroutine()), "HV")
		time.Sleep(time.Second * 3)
	}
}

func main() {
	flag.Parse()

	channelTypes := []uint32{pbase.RIDE_SHARE, pbase.PEOPLE_AGENT_SVC, pbase.GEOGRAPHIC_SVC, pbase.MQTT_GATEWAY_SVC}
	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "HarmoVisObjMap", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connectin SynerexServer at [%s]\n", sxServerAddress)

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	wg := sync.WaitGroup{} // for syncing other goroutines

	ioserv = run_server()
	fmt.Printf("Running HarmoVisObjMap Server.\n")
	if ioserv == nil {
		os.Exit(1)
	}

	client := sxutil.GrpcConnectServer(sxServerAddress) // if there is server address change, we should do it!

	argJSON := fmt.Sprintf("{Client:Map:RIDE")
	rideClient := sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJSON)

	argJSON2 := fmt.Sprintf("{Client:Map:PAGENT}")
	pa_client := sxutil.NewSXServiceClient(client, pbase.PEOPLE_AGENT_SVC, argJSON2)

	argJSON3 := fmt.Sprintf("{Client:Mp:Geo}")
	geo_client := sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJSON3)

	argJSON4 := fmt.Sprintf("{Client:MQTT}")
	mqtt_client := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJSON4)

	wg.Add(1)

	go subscribeRideSupply(rideClient)

	go subscribePAgentSupply(pa_client)

	go subscribeGeoSupply(geo_client)

	go subscribeMQTTSupply(mqtt_client)

	go monitorStatus() // keep status

	serveMux := http.NewServeMux()

	serveMux.Handle("/socket.io/", ioserv)
	serveMux.HandleFunc("/", assetsFileHandler)

	log.Printf("Starting Harmoware-VIS ObjMap Provider %s  on port %d", version, *port)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *port), serveMux)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
