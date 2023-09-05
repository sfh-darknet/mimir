// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	errorSampleRate = 5
	timestamp       = model.Time(1575043969)
	oooTimeWindow   = 2 * time.Hour
)

var (
	metricLabelAdapters = []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
)

func TestNewIngestErrMsgs(t *testing.T) {
	timestamp := model.Time(1575043969)
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}

	tests := map[string]struct {
		err error
		msg string
	}{
		"newIngestErrSampleTimestampTooOld": {
			err: newIngestErrSampleTimestampTooOld(timestamp, metricLabelAdapters),
			msg: `the sample has been rejected because its timestamp is too old (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrSampleTimestampTooOld_out_of_order_enabled": {
			err: newIngestErrSampleTimestampTooOldOOOEnabled(timestamp, metricLabelAdapters, 2*time.Hour),
			msg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of 2h (err-mimir-sample-timestamp-too-old). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrSampleOutOfOrder": {
			err: newIngestErrSampleOutOfOrder(timestamp, metricLabelAdapters),
			msg: `the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed (err-mimir-sample-out-of-order). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrSampleDuplicateTimestamp": {
			err: newIngestErrSampleDuplicateTimestamp(timestamp, metricLabelAdapters),
			msg: `the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested (err-mimir-sample-duplicate-timestamp). The affected sample has timestamp 1970-01-19T05:30:43.969Z and is from series {__name__="test"}`,
		},
		"newIngestErrExemplarMissingSeries": {
			err: newIngestErrExemplarMissingSeries(timestamp, metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}),
			msg: `the exemplar has been rejected because the related series has not been ingested yet (err-mimir-exemplar-series-missing). The affected exemplar is {traceID="123"} with timestamp 1970-01-19T05:30:43.969Z for series {__name__="test"}`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.msg, tc.err.Error())
		})
	}
}

func testWrappedError(t *testing.T, emptySamplerWrapper func(error) error, samplerWrapper func(error) error, err error) {
	require.Error(t, err)

	wrappedError := emptySamplerWrapper(err)
	errors.Is(wrappedError, err)

	wrappedError = samplerWrapper(err)
	require.Error(t, wrappedError)
	require.Errorf(t, wrappedError, fmt.Sprintf("%s (sampled 1/%d)", err.Error(), errorSampleRate))
}

func TestIngesterErrSamplersWrapper(t *testing.T) {
	emptySamplers := newIngesterErrSamplers(0)
	samplers := newIngesterErrSamplers(errorSampleRate)

	tests := map[string]struct {
		emptySamplerWraper func(error) error
		samplerWrapper     func(error) error
		err                error
	}{
		"WrapSampleTimestampTooOldError": {
			emptySamplerWraper: emptySamplers.WrapSampleTimestampTooOldError,
			samplerWrapper:     samplers.WrapSampleTimestampTooOldError,
			err:                newIngestErrSampleTimestampTooOld(timestamp, metricLabelAdapters),
		},
		"WrapSampleTimestampTooOldOOOEnabledError": {
			emptySamplerWraper: emptySamplers.WrapSampleTimestampTooOldOOOEnabledError,
			samplerWrapper:     samplers.WrapSampleTimestampTooOldOOOEnabledError,
			err:                newIngestErrSampleTimestampTooOldOOOEnabled(timestamp, metricLabelAdapters, oooTimeWindow),
		},
		"WrapSampleTimestampTooFarInFutureError": {
			emptySamplerWraper: emptySamplers.WrapSampleTimestampTooFarInFutureError,
			samplerWrapper:     samplers.WrapSampleTimestampTooFarInFutureError,
			err:                newIngestErrSampleTimestampTooFarInFuture(model.Time(time.Now().UnixMilli()+(86400*1000)), metricLabelAdapters),
		},
		"WrapSampleOutOfOrderError": {
			emptySamplerWraper: emptySamplers.WrapSampleOutOfOrderError,
			samplerWrapper:     samplers.WrapSampleOutOfOrderError,
			err:                newIngestErrSampleOutOfOrder(model.Time(9), metricLabelAdapters),
		},
		"WrapSampleDuplicateTimestampError": {
			emptySamplerWraper: emptySamplers.WrapSampleDuplicateTimestampError,
			samplerWrapper:     samplers.WrapSampleDuplicateTimestampError,
			err:                newIngestErrSampleDuplicateTimestamp(timestamp, metricLabelAdapters),
		},
		"WrapMaxSeriesPerMetricLimitExceededError": {
			emptySamplerWraper: emptySamplers.WrapMaxSeriesPerMetricLimitExceededError,
			samplerWrapper:     samplers.WrapMaxSeriesPerMetricLimitExceededError,
			err:                errMaxSeriesPerMetricLimitExceeded,
		},
		"WrapMaxMetadataPerMetricLimitExceededError": {
			emptySamplerWraper: emptySamplers.WrapMaxMetadataPerMetricLimitExceededError,
			samplerWrapper:     samplers.WrapMaxMetadataPerMetricLimitExceededError,
			err:                fmt.Errorf("max metadata per metric limit exceeded"),
		},
		"WrapMaxSeriesPerUserLimitExceededError": {
			emptySamplerWraper: emptySamplers.WrapMaxSeriesPerUserLimitExceededError,
			samplerWrapper:     samplers.WrapMaxSeriesPerUserLimitExceededError,
			err:                errMaxSeriesPerUserLimitExceeded,
		},
		"WrapMaxMetadataPerUserLimitExceededError": {
			emptySamplerWraper: emptySamplers.WrapMaxMetadataPerUserLimitExceededError,
			samplerWrapper:     samplers.WrapMaxMetadataPerUserLimitExceededError,
			err:                fmt.Errorf("max metadata per user limit exceeded"),
		},
	}

	for _, testData := range tests {
		testWrappedError(t, testData.emptySamplerWraper, testData.samplerWrapper, testData.err)
	}
}
