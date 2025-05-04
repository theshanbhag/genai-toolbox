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

package mongodb

import (
    "context"
    "fmt"

    "github.com/googleapis/genai-toolbox/internal/sources"
    "github.com/googleapis/genai-toolbox/internal/tools"
	mongodbsrc "github.com/googleapis/genai-toolbox/internal/sources/mongodb"
    "go.mongodb.org/mongo-driver/mongo"
)

const ToolKind string = "mongodb-atlas"


type Config struct {
    Name         string           `yaml:"name" validate:"required"`
    Kind         string           `yaml:"kind" validate:"required"`
    Source       string           `yaml:"source" validate:"required"`
    Description  string           `yaml:"description" validate:"required"`
    Collection   string           `yaml:"collection" validate:"required"`
    Query        map[string]any   `yaml:"query" validate:"required"`
    AuthRequired []string         `yaml:"authRequired"`
    Parameters   tools.Parameters `yaml:"parameters"`
}

// validate interface
var _ tools.ToolConfig = Config{}

func (cfg Config) ToolConfigKind() string {
    return ToolKind
}

func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
    // verify source exists
    rawS, ok := srcs[cfg.Source]
    if !ok {
        return nil, fmt.Errorf("no source named %q configured", cfg.Source)
    }

    // verify the source is compatible
    s, ok := rawS.(*mongodbsrc.Source)
    if !ok {
        return nil, fmt.Errorf("invalid source for %q tool", ToolKind)
    }

    mcpManifest := tools.McpManifest{
        Name:        cfg.Name,
        Description: cfg.Description,
        InputSchema: cfg.Parameters.McpManifest(),
    }

    // finish tool setup
t := Tool{
	Name:         cfg.Name,
	Kind:         ToolKind,
	Parameters:   cfg.Parameters,
	Collection:   cfg.Collection,
	Query:        cfg.Query,
	AuthRequired: cfg.AuthRequired,
	Client:       s.Client,
	Database:     s.DatabaseName(),
	manifest:     tools.Manifest{Description: cfg.Description, Parameters: cfg.Parameters.Manifest(), AuthRequired: cfg.AuthRequired},
	mcpManifest:  mcpManifest,
}
    return t, nil
}

// validate interface
var _ tools.Tool = Tool{}

type Tool struct {
    Name         string           `yaml:"name"`
    Kind         string           `yaml:"kind"`
    AuthRequired []string         `yaml:"authRequired"`
    Parameters   tools.Parameters `yaml:"parameters"`

    Client      *mongo.Client
    Database    string
    Collection  string
    Query       map[string]any
    manifest    tools.Manifest
    mcpManifest tools.McpManifest
}

func (t Tool) Invoke(ctx context.Context, params tools.ParamValues) ([]any, error) {
    collection := t.Client.Database(t.Database).Collection(t.Collection)
    filter := t.Query

    // Apply parameters to the query
    for key, value := range params.AsMap() {
        filter[key] = value
    }

    cursor, err := collection.Find(ctx, filter)
    if err != nil {
        return nil, fmt.Errorf("unable to execute query: %w", err)
    }
    defer cursor.Close(ctx)

    var results []any
    for cursor.Next(ctx) {
        var doc map[string]any
        if err := cursor.Decode(&doc); err != nil {
            return nil, fmt.Errorf("unable to parse document: %w", err)
        }
        results = append(results, doc)
    }

    if err := cursor.Err(); err != nil {
        return nil, fmt.Errorf("cursor error: %w", err)
    }

    return results, nil
}

func (t Tool) ParseParams(data map[string]any, claims map[string]map[string]any) (tools.ParamValues, error) {
    return tools.ParseParams(t.Parameters, data, claims)
}

func (t Tool) Manifest() tools.Manifest {
    return t.manifest
}

func (t Tool) McpManifest() tools.McpManifest {
    return t.mcpManifest
}

func (t Tool) Authorized(verifiedAuthServices []string) bool {
    return tools.IsAuthorized(t.AuthRequired, verifiedAuthServices)
}