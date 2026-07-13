/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package tracing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewProviderEmptyEndpoint(t *testing.T) {
	provider, err := NewProvider(context.Background(), "")
	require.NoError(t, err)
	require.Nil(t, provider)
}

func TestStartWithErrorNilSafe(t *testing.T) {
	ctx, end := StartWithError(nil, nil, SpanApplyBatch, DMLEventAttributes("db", "tbl", "insert")...)
	require.NotNil(t, ctx)
	end(nil)
}

func TestStartWithErrorRecordsError(t *testing.T) {
	ctx, end := StartWithError(context.Background(), NoopProvider, SpanExecuteSQL)
	end(errors.New("boom"))
	require.NotNil(t, ctx)
}

func TestDMLEventAttributes(t *testing.T) {
	attrs := DMLEventAttributes("mydb", "orders", "update")
	require.Len(t, attrs, 3)
}
