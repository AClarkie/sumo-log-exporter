package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/AClarkie/sumo-log-exporter/pkg/query"
	"github.com/remeh/sizedwaitgroup"
	"github.com/spf13/viper"
)

type config struct {
	Concurrency int `yaml:"concurrency"`
	Sumo        struct {
		ApiURL string `yaml:"apiUrl"`
		Query  struct {
			Statement string `yaml:"statement"`
			StartDate string `yaml:"startDate"`
			EndDate   string `yaml:"endDate"`
			TimeZone  string `yaml:"timeZone"`
		} `yaml:"query"`
	} `yaml:"sumo"`
	Filename string `yaml:"filename"`
	S3       struct {
		Enabled        bool   `yaml:"enabled"`
		Bucket         string `yaml:"bucket"`
		DeleteOnUpload bool   `yaml:"deleteOnUpload"`
	} `yaml:"s3"`
	ACCESS_ID             string `yaml:"ACCESS_ID"`
	ACCESS_KEY            string `yaml:"ACCESS_KEY"`
	AWS_REGION            string `yaml:"AWS_REGION"`
	AWS_ACCESS_KEY_ID     string `yaml:"AWS_ACCESS_KEY_ID"`
	AWS_SECRET_ACCESS_KEY string `yaml:"AWS_SECRET_ACCESS_KEY"`
}

var (
	conf       *config
	dateFormat = "2006-01-02T15:04:05"
)

func main() {
	fmt.Println("Starting sumo-log-exporter")
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error : %s", err)
		os.Exit(1)
	}
}

func run() error {
	// Generate job search queries to run
	queries, err := splitByDay(conf.Sumo.Query.StartDate, conf.Sumo.Query.EndDate)
	if err != nil {
		return err
	}

	// Setup sized wait group
	swg := sizedwaitgroup.New(conf.Concurrency)
	errs := make(chan error)
	for i, queryToRun := range queries {
		swg.Add()

		go func(q *query.JobSearch, index int) error {
			defer swg.Done()

			fmt.Printf("Starting execution and export to CSV of search job: %d \n", index)
			err = q.ExecuteSearchJob()

			if err != nil {
				errs <- err
			}

			err = q.ExportToCSV()
			fmt.Printf("Export complete for search job %d, total messages exported: %d \n", index, q.JobState.MessageCount)

			if err != nil {
				errs <- err
			}

			if conf.S3.Enabled {
				fmt.Printf("Uploading files for search job: %d \n", index)
				err = q.UploadFileToS3()
				fmt.Printf("S3 upload complete for search job: %d \n", index)
				if err != nil {
					errs <- err
				}
			}

			err = q.DeleteSearchJob()
			if err != nil {
				errs <- err
			}

			return nil
		}(queryToRun, i)
	}

	// wait until all the fetches are done and close the error
	// channel so the loop below terminates
	go func() {
		swg.Wait()
		close(errs)
	}()

	// return the first error
	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func splitByDay(startTime string, endTime string) ([]*query.JobSearch, error) {
	start, err := time.Parse(dateFormat, startTime)
	if err != nil {
		return nil, fmt.Errorf("invalid startDate format %s", err)
	}

	end, err := time.Parse(dateFormat, endTime)
	if err != nil {
		return nil, fmt.Errorf("invalid endDate format %s", err)
	}

	// If less than a day no need to split
	if (end.Sub(start).Hours() / 24) < 1 {
		return []*query.JobSearch{
			createQuery(conf.Sumo.Query.StartDate, conf.Sumo.Query.EndDate),
		}, nil
	}

	queries := []*query.JobSearch{
		createQuery(conf.Sumo.Query.StartDate, start.AddDate(0, 0, 1).Format(dateFormat)),
	}

	for {
		iStart := start.AddDate(0, 0, 1)
		iEnd := start.AddDate(0, 0, 2)

		queries = append(queries, createQuery(iStart.Format(dateFormat), iEnd.Format(dateFormat)))
		if iEnd.Unix() >= end.Unix() {
			break
		}
		start = iStart
	}

	return queries, nil
}

func createQuery(startDate, endDate string) *query.JobSearch {
	return query.NewQuery(conf.Sumo.ApiURL, conf.ACCESS_ID, conf.ACCESS_KEY, conf.AWS_REGION, conf.S3.Bucket, conf.S3.Enabled, conf.S3.DeleteOnUpload, conf.Filename+"_"+startDate+".csv", conf.Sumo.Query.Statement, startDate, endDate, conf.Sumo.Query.TimeZone)
}

func init() {
	viper.SetConfigFile("config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	// Override config with values set as env vars
	viper.AutomaticEnv()

	if err := viper.Unmarshal(&conf); err != nil {
		log.Fatalf("unable to unmarshall the config %v", err)
	}
}
