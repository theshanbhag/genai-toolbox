// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/googleapis/genai-toolbox/tests"
)

const (
	mongodbSourceKind = "mongodb"
	mongodbToolKind   = "mongodb-atlas"
)

var (
	mongodbUser           = os.Getenv("MONGODB_USER")
	mongodbPass           = os.Getenv("MONGODB_PASS")
	mongodbHost           = os.Getenv("MONGODB_HOST")
	mongodbDatabase       = os.Getenv("MONGODB_DATABASE")
	mongodbCollection     = os.Getenv("MONGODB_COLLECTION")
	SERVICE_ACCOUNT_EMAIL = os.Getenv("SERVICE_ACCOUNT_EMAIL")
)

// getCouchbaseVars validates and returns Couchbase configuration variables
func getMongoDBVars(t *testing.T) map[string]any {
	switch "" {
	case mongodbUser:
		t.Fatal("'MONGODB_USER' not set")
	case mongodbPass:
		t.Fatal("'MONGODB_PASS' not set")
	case mongodbHost:
		t.Fatal("'MONGODB_HOST' not set")
	case mongodbDatabase:
		t.Fatal("'MONGODB_DATABASE' not set")
	}

	uri := fmt.Sprintf("mongodb+srv://%s:%s@%s", mongodbUser, mongodbPass, mongodbHost)
	return map[string]any{
		"kind":     mongodbSourceKind,
		"uri":      uri,
		"database": mongodbDatabase,
	}
}

func TestMongoDBToolEndpoints(t *testing.T) {
	sourceConfig := getMongoDBVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// TODO 1: Add set up test database function calls

	// TODO 2: Add helper function calls to get test Tool statements

	// TODO 3:  Pass to `tests.GetToolsConfig` and get a toolsFile
	toolsFile := tests.GetToolsConfig(sourceConfig, mongodbToolKind, paramToolStatement, authToolStatement)

	cmd, cleanup, err := tests.StartCmd(ctx, toolsFile, args...)
	if err != nil {
		t.Fatalf("command initialization returned an error: %s", err)
	}
	defer cleanup()

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := cmd.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`))
	if err != nil {
		t.Logf("toolbox command logs: \n%s", out)
		t.Fatalf("toolbox didn't start successfully: %s", err)
	}

	// Run generic Tool get test suites
	tests.RunToolGetTest(t)

	// TODO 4: Set up wanted strings for tests
	select1Want := ""
	failMcpInvocationWant := ""

	// Run generic Tool invocation test suites
	invokeParamWant, mcpInvokeParamWant := tests.GetNonSpannerInvokeParamWant()
	tests.RunToolInvokeTest(t, select1Want, invokeParamWant)
	tests.RunMCPToolCallMethod(t, mcpInvokeParamWant, failMcpInvocationWant)
}
