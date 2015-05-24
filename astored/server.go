package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/skyec/astore"
	"github.com/skyec/astore/consumer/kafka"
)

func main() {
	var (
		storeDir     string
		listenAddr   string
		kafkaEnabled bool
		kafkaBrokers *flagKafkaBrokers = &flagKafkaBrokers{}
		kafkaTopic   string
		purge        bool
	)

	flag.StringVar(&storeDir, "s", "/var/astore", "Directory that contains the store data")
	flag.BoolVar(&purge, "PURGE", false, "Purge the store of all data. WARNING: you can't recover from this!!")
	flag.StringVar(&listenAddr, "l", ":9898", "Port the main service listens on")
	flag.BoolVar(&kafkaEnabled, "K", false, "Enable consuming events from Kafka")
	flag.StringVar(&kafkaTopic, "topic", "astore", "Kafka topic to consume events from")
	flag.Var(kafkaBrokers, "brokers", "List of Kafka brokers if enabled e.g. kafka://b1:9092,b2:9092")

	// TODO: add a flag for the list of partitions to consume. Right now only partion zero is consumed.

	flag.Parse()

	store, err := astore.NewReadWriteableStore(storeDir)
	if err != nil {
		log.Fatalln("Error initializing the store:", err)
	}

	if purge {
		log.Println("Purging the datastore at:", storeDir)
		store.Purge()
		log.Println("Done.")
		os.Exit(1)
	}

	var vars MuxVars = mux.Vars

	r := mux.NewRouter()
	r.NotFoundHandler = Handle404{}

	r.Handle("/v1/keys/{key}", NewAppendHandler(store, vars)).Methods("POST")
	r.Handle("/v1/keys/{key}", NewReadallHandler(store, vars)).Methods("GET")

	log.Println("Starting ...")
	log.Println("Listening on:", listenAddr)
	log.Println("Store directory:", storeDir)

	if kafkaEnabled {
		kafkaTopic = strings.TrimSpace(kafkaTopic)
		if len(kafkaBrokers.brokers) < 1 || kafkaTopic == "" {
			log.Fatalln("Missing brokers or topic.")
		}

		kconf := kafka.NewConfig()
		kconf.Topic = kafkaTopic
		kconf.Brokers = kafkaBrokers.brokers

		kafka, err := kafka.NewKafka(store, kconf, nil)
		if err != nil {
			log.Fatalln("Error initializing Kafka consumer:", err)
		}
		err = kafka.Run()
		if err != nil {
			log.Fatalln("Error starting Kafka consumer:", err)
		}

		log.Print("The Kafka consumer is ENABLED")
		log.Println("Kafka brokers:", kafkaBrokers.String())
		log.Println("Kafka topic:", kafkaTopic)
	}

	http.Handle("/", r)
	http.ListenAndServe(listenAddr, nil)

}

// RequestVars is the interface implemented by objects that know how to parse parameters
// out of the request (URL etc)
type RequestVars interface {
	Vars(r *http.Request) map[string]string
}

type MuxVars func(r *http.Request) map[string]string

func (m MuxVars) Vars(r *http.Request) map[string]string {
	return m(r)
}

func writeOKResponse(w http.ResponseWriter, r *http.Request, payload interface{}) {
	buf, err := json.Marshal(payload)
	if err != nil {
		// TODO: need a proper error handler for this
		http.Error(w, "json error", http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(buf); err != nil {
		log.Println("Socket write error!", err)
	}

	logRequest(r, http.StatusOK)
}

// flagKafkaBrokers implements the flag.Value interface to extract a list of kafka broker
// addresses from a commandline flag.
type flagKafkaBrokers struct {
	brokers  []string
	original string
}

const (
	KAFKA_HOST_SCHEME = "kafka://"
	KAFKA_BROKER_SEP  = ","
	KAFKA_PORT_SEP    = ':'
)

// Set expects the format of value to be "kafka://addr:port,addr:port,addr:port"
func (fkb *flagKafkaBrokers) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("Invalid kafka broker list.")
	}

	if !strings.HasPrefix(value, KAFKA_HOST_SCHEME) {
		return fmt.Errorf("Invalid kafka broker list.")
	}

	value = strings.TrimPrefix(value, KAFKA_HOST_SCHEME)
	if value == "" {
		return fmt.Errorf("Invalid kafaka broker list.")
	}

	brokers := strings.Split(value, KAFKA_BROKER_SEP)
	if len(brokers) < 1 {
		return fmt.Errorf("No brokeres in list")
	}

	for _, broker := range brokers {
		if broker == "" {
			continue
		}
		if strings.IndexRune(broker, ':') == -1 {
			return fmt.Errorf("Invalid broker '%s': missing port seperator", broker)
		}
		fkb.brokers = append(fkb.brokers, broker)
	}
	return nil
}

func (fkb *flagKafkaBrokers) String() string {
	return strings.Join(fkb.brokers, KAFKA_BROKER_SEP)
}
