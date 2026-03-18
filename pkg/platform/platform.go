package platform

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
)

type Artifact struct {
	Id            string    `json:"id"`
	DatasetName   string    `json:"datasetName"`
	ProducerId    string    `json:"producerId"`
	Version       string    `json:"version"`
	SchemaVersion string    `json:"schemaVersion"`
	URI           string    `json:"uri"`
	PublishedAt   time.Time `json:"publishedAt"`
}

func NewArtifact(datasetName, producerId, version, schemaVersion, uri string) Artifact {
	return Artifact{
		Id:            uuid.New().String(),
		DatasetName:   datasetName,
		ProducerId:    producerId,
		Version:       version,
		SchemaVersion: schemaVersion,
		URI:           uri,
		PublishedAt:   time.Now().Round(0),
	}
}

type Catalog struct {
	fd *os.File
}

func NewCatalog(path string) (*Catalog, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	return &Catalog{fd}, nil
}

var ErrArtifactNotFound = errors.New("artifact not found")

func (c *Catalog) SaveArtifact(a Artifact) error {
	artifacts, err := loadArtifacts(c.fd)
	if err != nil {
		return err
	}

	updated := false

	for i, art := range artifacts {
		if art.Id == a.Id {
			artifacts[i] = &a
			updated = true
		}
	}

	if !updated {
		artifacts = append(artifacts, &a)
	}

	return writeJson(c.fd, artifacts)
}

func (c *Catalog) GetArtifact(id string) (*Artifact, error) {
	artifacts, err := loadArtifacts(c.fd)
	if err != nil {
		return nil, err
	}

	for _, a := range artifacts {
		if a.Id == id {
			return a, nil
		}
	}

	return nil, ErrArtifactNotFound
}

func (c *Catalog) ListArtifact(datasetName string) ([]*Artifact, error) {
	artifacts, err := loadArtifacts(c.fd)
	if err != nil {
		return nil, err
	}

	var out []*Artifact
	for _, a := range artifacts {
		if a.DatasetName == datasetName {
			out = append(out, a)
		}
	}
	return out, nil
}

func loadArtifacts(fd *os.File) ([]*Artifact, error) {
	data, err := os.ReadFile(fd.Name())
	if err != nil {
		return nil, err
	}

	var a []*Artifact
	if len(data) > 0 {
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func writeJson(f *os.File, v any) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := f.Truncate(0); err != nil {
		return err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := json.NewEncoder(f).Encode(v); err != nil {
		return err
	}

	return f.Sync()
}
