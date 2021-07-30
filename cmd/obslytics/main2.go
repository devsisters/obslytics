// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/exporter"
	"github.com/thanos-community/obslytics/pkg/exporter/parquet"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"gopkg.in/yaml.v2"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-community/obslytics/pkg/series/promread"
	"github.com/thanos-io/thanos/pkg/http"
)

// 공통 변수
const (
	prometheusEndpoint = "http://localhost:9090/api/v1/read"
	contextTimeout = 1
)

// 로컬 파일 아웃풋용 변수
const (
	localParquetOutputPathForTest = "/Users/devsisters/github/obslytics"
	localParquetOutputNameForTest = "mytest.parquet"
)

const (
	s3ParquetBucketNameForTest = "devsisters-alicek106-isms-test"
)

func seriesToS3Parquet(maxt time.Time, mint time.Time, metricMatchers []*labels.Matcher) {
	logger := log.NewLogfmtLogger(os.Stderr)

	// 버킷 (output parquet가 저장될 공간에 대한 인터페이스) 선언
	storageConf, err := yaml.Marshal(client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:             s3ParquetBucketNameForTest,
			Endpoint: "s3.ap-northeast-1.amazonaws.com",
			Region: "ap-northeast-1",
			PartSize: 134217728,
		},
	})

	// local file output일때 component는 쓸모 없음
	bkt, err := client.NewBucket(logger, storageConf, nil, "my-test-component")
	if err != nil{
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout * time.Minute)
	defer cancel()

	// 프로메테우스에서 메트릭 읽기
	api, err := promread.NewSeries(logger, series.Config{
		Endpoint:  prometheusEndpoint,
		TLSConfig: http.TLSConfig{InsecureSkipVerify: true},
	})

	if err != nil{
		fmt.Println(err)
	}

	s, _ := api.Read(ctx, series.Params{
		Matchers: metricMatchers,
		MinTime: mint,
		MaxTime: maxt,
	})

	// df로 변환
	df, _ := dataframe.FromSeries(s, contextTimeout * time.Second, func(o *dataframe.AggrsOptions) {
		o.Count.Enabled = true
		o.Sum.Enabled = true
		o.Min.Enabled = true
		o.Max.Enabled = true
	})

	// local file로 출력
	err = exporter.New(parquet.NewEncoder(), localParquetOutputNameForTest, bkt).Export(ctx, df)
	if err != nil{
		fmt.Println(err)
	}
}

func seriesToFileParquet(maxt time.Time, mint time.Time, metricMatchers []*labels.Matcher) {
	logger := log.NewLogfmtLogger(os.Stderr)

	// 버킷 (output parquet가 저장될 공간에 대한 인터페이스) 선언
	storageConf, err := yaml.Marshal(client.BucketConfig{
		Type: client.FILESYSTEM,
		Config: filesystem.Config{Directory: localParquetOutputPathForTest},
	})

	// local file output일때 component는 쓸모 없음
	bkt, err := client.NewBucket(logger, storageConf, nil, "")
	if err != nil{
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout * time.Minute)
	defer cancel()

	// 프로메테우스에서 메트릭 읽기
	api, err := promread.NewSeries(logger, series.Config{
		Endpoint:  prometheusEndpoint,
		TLSConfig: http.TLSConfig{InsecureSkipVerify: true},
	})

	if err != nil{
		fmt.Println(err)
	}

	s, _ := api.Read(ctx, series.Params{
		Matchers: metricMatchers,
		MinTime: mint,
		MaxTime: maxt,
	})

	// df로 변환
	df, _ := dataframe.FromSeries(s, contextTimeout * time.Second, func(o *dataframe.AggrsOptions) {
		o.Count.Enabled = true
		o.Sum.Enabled = true
		o.Min.Enabled = true
		o.Max.Enabled = true
	})

	// local file로 출력
	err = exporter.New(parquet.NewEncoder(), localParquetOutputNameForTest, bkt).Export(ctx, df)
	if err != nil{
		fmt.Println(err)
	}
}


func main() {
	maxt := time.Now()
	mint := maxt.AddDate(0, 0, -1)

	metricMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "prometheus_tsdb_head_series"),
	}

	seriesToS3Parquet(maxt, mint, metricMatchers)
}