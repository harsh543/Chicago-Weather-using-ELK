package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/olivere/elastic"
	"go.uber.org/zap"
)

var (
	logger *zap.Logger
)

type WeatherMeasurement struct {
	ID                         string  `json:"id"`
	Timestamp                  string  `json:"date"`
	TemperatureCelsius         float32 `json:"temperature_celsius"`
	TemperatureFahrenheit      int32   `json:"temperature_fahrenheit"`
	HumidityPercentage         int32   `json:"humidity_percentage"`
	RainIntensityMMPerHour     float32 `json:"rain_intensity_mm_per_hour"`
	RainIntensityInchesPerHour float32 `json:"rain_intensity_inches_per_hour"`
	RainSinceLastHourMM        float32 `json:"rain_since_last_hour_mm"`
	RainSinceLastHourInches    float32 `json:"rain_since_last_hour_inches"`
	PrecipitationType          int32   `json:"precipitation_type"`
}

// checkforErrors is invoked by bulk processor after every commit.
// The err variable indicates success or failure.
func checkForErrors(executionId int64,
	requests []elastic.BulkableRequest,
	response *elastic.BulkResponse,
	err error) {
	if err != nil {
		logger.Error("Bulk Insert Error",
			zap.String("error", err.Error()))
		os.Exit(1)
	}
}

func main() {
	logger, _ = zap.NewProduction()
	defer logger.Sync()
	logger.Info("Elastic loader ... starting")

	deleteFlag := flag.Bool("delete", false, "Delete index")
	flag.Parse()

	ctx := context.Background()

	// Elastic Client
	elasticEndpoint := os.Getenv("ELASTIC_ENDPOINT")
	elasticUsername := os.Getenv("ELASTIC_USERNAME")
	elasticPassword := os.Getenv("ELASTIC_PASSWORD")
	logger.Info("Elastic server",
		zap.String("url", elasticEndpoint))
	elasticClient, err := elastic.NewClient(elastic.SetURL(elasticEndpoint),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(elasticUsername, elasticPassword))
	if err != nil {
		logger.Error("Error initializing Elastic client",
			zap.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("Initialized Elastic client")

	indexName := "chicago-weather"

	// Delete
	if *deleteFlag {
		indices := []string{indexName}
		for _, indexName := range indices {
			destroyIndex, err := elasticClient.DeleteIndex(indexName).Do(ctx)
			if err != nil {
				logger.Error("Failed to delete Elastic index",
					zap.String("index", indexName),
					zap.String("error", err.Error()))
				os.Exit(1)
			} else {
				if !destroyIndex.Acknowledged {
					logger.Error("Failed to acknowledge deletion of Elastic index",
						zap.String("index", indexName),
						zap.String("error", err.Error()))
					os.Exit(1)
				} else {
					logger.Info("Index deleted",
						zap.String("index", indexName))
				}
			}
		}
		os.Exit(0)
	}

	// Check for Index, create if necessary
	exists, err := elasticClient.IndexExists(indexName).Do(ctx)
	if err != nil {
		logger.Error("Failed to see if index exists",
			zap.String("index", indexName),
			zap.String("error", err.Error()))
		os.Exit(1)
	}
	if !exists {
		mapping, err := ioutil.ReadFile("mapping.json")
		createIndex, err := elasticClient.CreateIndex(indexName).
			BodyString(string(mapping)).Do(ctx)
		if err != nil {
			logger.Error("Error loading mapping",
				zap.String("index", indexName),
				zap.String("error", err.Error()))
			os.Exit(1)
		} else {
			if !createIndex.Acknowledged {
				logger.Error("Create index not acknowledged",
					zap.String("index", indexName),
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			logger.Info("Index created",
				zap.String("index", indexName))
		}
	} else {
		logger.Info("Index already exists",
			zap.String("index", indexName))
	}

	// Bulk Processor
	bulkProc, err := elasticClient.
		BulkProcessor().
		Name("Worker").
		Workers(4).
		After(checkForErrors).
		Do(context.Background())

	// Weather Measurements
	weatherFiles := []string{
		"oak2017.csv",
		//"Beach_Weather_Stations_-_Automated_Sensors.csv",
	}

	for _, weatherFile := range weatherFiles {
		logger.Info("Parsing file", zap.String("name", weatherFile))
		csvFile, _ := os.Open(weatherFile)
		reader := csv.NewReader(bufio.NewReader(csvFile))

		for {
			line, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				logger.Error("Reading Trip CSV file",
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			if line[0] == "Station Name" {
				continue
			}
			measurement := WeatherMeasurement{}
			measurement.ID = line[17]

			// Convert Measurement Timestamp from CST to ISO-8601 format
			loc, err := time.LoadLocation("America/Chicago")
			if err != nil {
				logger.Error("Error getting timezone of America/Chicago",
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			isoTime := fmt.Sprintf("%s %s", line[1], time.Now().In(loc).Format("-0700 MST"))
			parsedTime, err := time.Parse("01/02/2006 03:04:05 PM -0700 MST", isoTime)
			if err != nil {
				logger.Error("Error parsing StartTime into Time",
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			measurement.Timestamp = parsedTime.Format(time.RFC3339)

			// Convert Temperature from String to Float
			temp, err := strconv.ParseFloat(line[2], 64)
			if err != nil {
				logger.Error("Converting Air Temperature to float",
					zap.String("id", line[2]),
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			measurement.TemperatureCelsius = float32(temp)
			measurement.TemperatureFahrenheit = int32(temp*1.8 + 32.0)

			// Convert Humidity from String to Int32
			humidity, err := strconv.Atoi(line[4])
			if err != nil {
				logger.Error("Converting Humidity to int",
					zap.String("id", line[4]),
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			measurement.HumidityPercentage = int32(humidity)

			// Convert Rain Intensity from String to Float
			ri, err := strconv.ParseFloat(line[5], 64)
			if err != nil {
				logger.Error("Converting Rain Intensity to float",
					zap.String("id", line[5]),
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			measurement.RainIntensityMMPerHour = float32(ri)
			measurement.RainIntensityInchesPerHour = float32(ri * 0.0393701)

			// Convert Interval Rain from String to Float
			ir, err := strconv.ParseFloat(line[6], 64)
			if err != nil {
				logger.Error("Converting Interval Rain to float",
					zap.String("id", line[6]),
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			measurement.RainSinceLastHourMM = float32(ir)
			measurement.RainSinceLastHourInches = float32(ir * 0.0393701)

			// Convert Precipitation Type from String to Int32
			pType, err := strconv.Atoi(line[8])
			if err != nil {
				logger.Error("Converting Precipitation Type to int",
					zap.String("id", line[8]),
					zap.String("error", err.Error()))
				os.Exit(1)
			}
			measurement.PrecipitationType = int32(pType)

			// Marshall the Weather Measurement into JSON and add to queue for Bulk API
			jsonM, err := json.Marshal(measurement)
			if err != nil {
				logger.Error("Error marshalling JSON",
					zap.String("error", err.Error()))
				os.Exit(1)
			} else {
				indexRequest := elastic.NewBulkIndexRequest().
					Index(indexName).
					Type("_doc").
					OpType("create").
					Id(measurement.ID).
					Doc(string(jsonM))
				bulkProc.Add(indexRequest)
			}
		}
	}

	err = bulkProc.Flush()
	if err != nil {
		logger.Error("Flushing Bulk Processor",
			zap.String("error", err.Error()))
		os.Exit(1)
	}
}
