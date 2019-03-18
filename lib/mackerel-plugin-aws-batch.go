package mpawsbatch

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
	mp "github.com/mackerelio/go-mackerel-plugin-helper"
)

// GraphDefinition of AwsBatchPlugin
func (p AwsBatchPlugin) GraphDefinition() map[string](mp.Graphs) {
	graphdef := map[string](mp.Graphs){
		"aws.batch.jobs.#": mp.Graphs{
			Label: "AWS Batch Jobs",
			Unit:  "integer",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "SUBMITTED", Label: "SUBMITTED"},
				mp.Metrics{Name: "PENDING", Label: "PENDING"},
				mp.Metrics{Name: "RUNNABLE", Label: "RUNNABLE"},
				mp.Metrics{Name: "STARTING", Label: "STARTING"},
				mp.Metrics{Name: "RUNNING", Label: "RUNNING"},
			},
		},
		"aws.batch.runtime.#": mp.Graphs{
			Label: "AWS Batch Jobs Runtime",
			Unit:  "float",
			Metrics: [](mp.Metrics){
				mp.Metrics{Name: "*", Label: "%2"},
			},
		},
	}
	return graphdef
}

type jobQueueNames []string

type AwsBatchPlugin struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Batch           *batch.Batch
	JobQueues       jobQueueNames
}

// FetchMetrics fetch the metrics
func (p AwsBatchPlugin) FetchMetrics() (map[string]interface{}, error) {
	stat := make(map[string]interface{})
	statuses := []string{"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}

	for _, name := range p.JobQueues {
		for _, s := range statuses {
			// get ListJobs
			lj, err := p.getListJobs(name, s)
			if err != nil {
				return nil, err
			}
			// get LastPoint from ListJobs
			n := p.getLastPoint(lj)
			stat["aws.batch.jobs."+name+"."+s] = n
			// get Runtime from ListJobs
			if s == "RUNNING" {
				for _, js := range lj.JobSummaryList {
					rt := p.getRuntime(js)
					stat["aws.batch.runtime."+name+"."+aws.StringValue(js.JobName)] = rt
				}
			}
		}
	}
	return stat, nil
}

func (p *AwsBatchPlugin) prepare() error {
	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	config := aws.NewConfig()
	if p.AccessKeyID != "" && p.SecretAccessKey != "" {
		config = config.WithCredentials(credentials.NewStaticCredentials(p.AccessKeyID, p.SecretAccessKey, ""))
	}
	if p.Region != "" {
		config = config.WithRegion(p.Region)
	}
	p.Batch = batch.New(sess, config)

	return nil
}

func (p AwsBatchPlugin) getListJobs(name string, status string) (*batch.ListJobsOutput, error) {
	input := &batch.ListJobsInput{
		JobQueue:  aws.String(name),
		JobStatus: aws.String(status),
	}
	resp, err := p.Batch.ListJobs(input)
	if err != nil {
		return nil, err
	}
	resp = resp.SetJobSummaryList(resp.JobSummaryList)
	return resp, nil
}

func (p AwsBatchPlugin) getLastPoint(lj *batch.ListJobsOutput) float64 {
	return float64(len(lj.JobSummaryList))
}

func (p AwsBatchPlugin) getRuntime(js *batch.JobSummary) float64 {
	sa := aws.MillisecondsTimeValue(js.StartedAt)
	d := time.Since(sa)
	return d.Minutes()
}

func (j *jobQueueNames) String() string {
	return fmt.Sprint("%v", *j)
}

func (j *jobQueueNames) Set(v string) error {
	*j = append(*j, v)
	return nil
}

func Do() {
	var plugin AwsBatchPlugin
	var jqn jobQueueNames

	optAccessKeyID := flag.String("access-key-id", "", "AWS Access Key ID")
	optSecretAccessKey := flag.String("secret-access-key", "", "AWS Secret Access Key")
	optRegion := flag.String("region", "", "AWS Batch Job Region")
	flag.Var(&jqn, "job-queue", "AWS Batch Job Queue Name")
	flag.Parse()

	plugin.AccessKeyID = *optAccessKeyID
	plugin.SecretAccessKey = *optSecretAccessKey
	plugin.Region = *optRegion
	plugin.JobQueues = jqn

	err := plugin.prepare()
	if err != nil {
		log.Fatalln(err)
	}

	helper := mp.NewMackerelPlugin(plugin)
	helper.Run()
}
