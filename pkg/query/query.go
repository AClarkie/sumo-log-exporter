package query

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type JobSearch struct {
	ApiEndpoint string
	Creds       string
	Filename    string

	JobQuery        *JobQuery
	JobState        *JobState
	S3Config        *S3Config
	JobMessageSlice *[]JobMessages `json:"messages"`
}

type S3Config struct {
	AWSRegion      string
	Bucket         string
	Enabled        bool
	DeleteOnUpload bool
}

type JobState struct {
	ID           string `json:"id"`
	State        string `json:"state"`
	MessageCount int    `json:"messageCount"`
	RecordCount  int    `json:"recordCount"`
}

type JobQuery struct {
	Query    string `json:"query"`
	From     string `json:"from"`
	To       string `json:"to"`
	TimeZone string `json:"timeZone"`
}

type JobMessages struct {
	Message JobMessageRaw `json:"map"`
}

type JobMessageRaw struct {
	MessageTime string `json:"_messagetime"`
	Host        string `json:"_sourcehost"`
	Source      string `json:"_source"`
	Log         string `json:"_raw"`
}

func NewQuery(apiEndpoint string, accessID string, accessKey string, awsRegion string, s3Bucket string, s3Enabled bool, deleteOnUpload bool, fileName string, query string, from string, to string, timezone string) *JobSearch {
	return &JobSearch{
		ApiEndpoint: apiEndpoint,
		Creds:       b64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", accessID, accessKey))),
		Filename:    fileName,
		S3Config: &S3Config{
			AWSRegion:      awsRegion,
			Bucket:         s3Bucket,
			Enabled:        s3Enabled,
			DeleteOnUpload: deleteOnUpload,
		},
		JobQuery: &JobQuery{
			Query:    query,
			From:     from,
			To:       to,
			TimeZone: timezone,
		},
	}
}

func (j *JobSearch) post(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("unable to create http POST request: %s", err)
	}

	return j.runRequest(req)
}

func (j *JobSearch) get(url string, body io.Reader, query map[string]string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return nil, fmt.Errorf("unable to create http GET request: %s", err)
	}

	// Generate url with query
	q := req.URL.Query()
	for k, v := range query {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	return j.runRequest(req)
}

func (j *JobSearch) delete(url string, query map[string]string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create http DELETE request: %s", err)
	}

	return j.runRequest(req)
}

func (j *JobSearch) runRequest(r *http.Request) (*http.Response, error) {
	r.Header.Add("Authorization", fmt.Sprintf("Basic %s", j.Creds))
	r.Header.Add("Content-Type", "application/json")

	client := http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		return nil, fmt.Errorf("unable to execute http %s request: %s", r.Method, err)
	}

	return resp, err
}

func (j *JobSearch) ExecuteSearchJob() error {
	requestBody, err := json.Marshal(j.JobQuery)
	if err != nil {
		return fmt.Errorf("could not marshal SearchJobQuery: %s", err)
	}

	response, err := j.post(j.ApiEndpoint, bytes.NewReader(requestBody))
	if err != nil {
		return err
	}
	defer response.Body.Close()

	var bdy []byte
	if response.StatusCode == 202 {
		bdy, _ = ioutil.ReadAll(response.Body)
	} else {
		return fmt.Errorf("%s", response.Status)
	}

	err = json.Unmarshal(bdy, &j.JobState)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body: %s", err)
	}

	return nil
}

func (j *JobSearch) refreshSearchJobState() error {

	if j.JobState == nil {
		return fmt.Errorf("could not refresh search job state as it's not been started")
	}

	response, err := j.get(j.ApiEndpoint+"/"+j.JobState.ID, nil, nil)
	if err != nil {
		return err
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("could not refresh search job: %s", response.Status)
	}

	var bdy []byte
	if response.StatusCode == 200 {
		bdy, _ = ioutil.ReadAll(response.Body)
	} else {
		return fmt.Errorf("%s", response.Status)
	}

	err = json.Unmarshal(bdy, &j.JobState)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body: %s", err)
	}

	return nil
}

func (j *JobSearch) getMessageBatch(query map[string]string) error {
	response, err := j.get(j.ApiEndpoint+"/"+j.JobState.ID+"/"+"messages", nil, query)
	if err != nil {
		return err
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(responseBody, &j)
	if err != nil {
		return fmt.Errorf("could not unmarshal response body: %s", err)
	}

	return nil
}

func (j *JobSearch) DeleteSearchJob() error {

	if j.JobState == nil {
		return fmt.Errorf("could not delete search job as it's not been started")
	}

	response, err := j.delete(j.ApiEndpoint+"/"+j.JobState.ID, nil)
	if err != nil {
		return err
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("could not delete search job: %s", response.Status)
	}

	return nil
}

func (j *JobSearch) ExportToCSV() error {
	var limit = "10000"
	var offset = 0
	var written = 0

	header := [][]string{
		{"message", "sourcehost", "source"},
	}

	f, err := os.Create(j.Filename)
	defer f.Close()

	if err != nil {
		return fmt.Errorf("failed to open csv: %s", err)
	}

	w := csv.NewWriter(f)
	err = w.WriteAll(header)
	if err != nil {
		return fmt.Errorf("failed to write to csv: %s", err)
	}

	for {
		query := map[string]string{"limit": limit, "offset": fmt.Sprintf("%d", offset)}

		err := j.getMessageBatch(query)
		if err != nil {
			return err
		}

		records := [][]string{}
		for _, message := range *j.JobMessageSlice {
			record := []string{
				message.Message.Log, message.Message.Host, message.Message.Source,
			}
			records = append(records, record)
		}
		err = w.WriteAll(records)

		if err != nil {
			return fmt.Errorf("failed to write to csv: %s", err)
		}

		written += len(*j.JobMessageSlice)

		// Continue if there are more messages to receive.
		if written < j.JobState.MessageCount {
			offset += len(*j.JobMessageSlice)
		} else {
			// If we're still collecting results then wait for job search to finish
			j.refreshSearchJobState()
			if j.JobState.State != "DONE GATHERING RESULTS" {
				time.Sleep(2 * time.Second)
				offset += len(*j.JobMessageSlice)
			} else if written >= j.JobState.MessageCount {
				break
			} else {
				offset += len(*j.JobMessageSlice)
			}
		}
	}

	return nil
}

func (j *JobSearch) UploadFileToS3() error {
	fileName := filepath.Base(j.Filename)
	upFile, err := os.Open(j.Filename)
	if err != nil {
		return fmt.Errorf("could not open local filepath [%v]: %+v", j.Filename, err)
	}
	defer upFile.Close()

	session, err := session.NewSession(&aws.Config{Region: aws.String(j.S3Config.AWSRegion)})
	if err != nil {
		return fmt.Errorf("could not initialize new aws session: %v", err)
	}

	s3Client := s3.New(session)
	uploader := s3manager.NewUploaderWithClient(s3Client, func(u *s3manager.Uploader) {
		u.PartSize = 100 * 1024 * 1024 // 100MB part size
		u.Concurrency = 10
		u.LeavePartsOnError = true // Don't delete the parts if the upload fails.
	})
	upParams := &s3manager.UploadInput{
		Bucket: &j.S3Config.Bucket,
		Key:    &fileName,
		Body:   upFile,
	}

	// Perform an upload.
	result, err := uploader.Upload(upParams)

	if err != nil {
		return fmt.Errorf("error uploading file [%v]: %+v", j.Filename, err)
	}
	fmt.Printf("file uploaded to, %s\n", result.Location)

	if j.S3Config.DeleteOnUpload {
		err = os.Remove(fileName)
		if err != nil {
			return fmt.Errorf("error removing file file [%v]: %+v", j.Filename, err)
		}
	}

	return nil
}
