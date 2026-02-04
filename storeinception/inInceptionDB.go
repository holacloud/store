package storeinception

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/holacloud/store"
)

type ConfigInceptionDB struct {
	Base       string `json:"base"`
	Collection string `json:"collection"`
	ApiKey     string `json:"api_key"`
	ApiSecret  string `json:"api_secret"`
}

type StoreInception[T store.Identifier] struct {
	config     *ConfigInceptionDB
	httpClient *http.Client
}

func New[T store.Identifier](config *ConfigInceptionDB) *StoreInception[T] {
	if config.Collection == "" {
		config.Collection = "items"
	}
	result := &StoreInception[T]{
		config: config,
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost:     100,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     60 * time.Second,

				// chatgpt recommendation :D
				ResponseHeaderTimeout: time.Second * 10, // Espera de respuesta
				TLSHandshakeTimeout:   time.Second * 5,  // Timeout en handshake TLS
				ExpectContinueTimeout: time.Second * 1,  // Timeout en "100-Continue"
			},
		},
	}
	result.dropCollection()
	result.ensureCollection()

	return result
}

type FindQuery struct {
	Filter  map[string]interface{} `json:"filter,omitempty"`
	Limit   int                    `json:"limit,omitempty"`
	Skip    int                    `json:"skip,omitempty"`
	Reverse bool                   `json:"reverse,omitempty"`
}

func (p *StoreInception[T]) List(ctx context.Context) ([]*T, error) {
	query := FindQuery{
		Filter: map[string]interface{}{},
		Limit:  -1,
	}
	payload, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	endpoint := p.config.Base + "/collections/" + url.PathEscape(p.config.Collection) + ":find"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Api-Key", p.config.ApiKey)
	req.Header.Set("Api-Secret", p.config.ApiSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("list: unexpected HTTP status: " + resp.Status)
	}

	var items []*T
	decoder := json.NewDecoder(resp.Body)
	// InceptionDB returns a stream of objects, one per line (JSON Lines)
	for {
		var item *T
		err := decoder.Decode(&item)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (p *StoreInception[T]) Put(ctx context.Context, item *T) error {

	// TODO: copy? remarshal?

	itemVersion := (*item).GetVersion()
	if itemVersion == 0 {
		// Assume the document is new

		(*item).SetVersion(itemVersion + 1) // 1
		payload, err := json.Marshal(item)
		(*item).SetVersion(itemVersion) // restore
		if err != nil {
			return err
		}
		endpoint := p.config.Base + "/collections/" + url.PathEscape(p.config.Collection) + ":insert"
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Api-Key", p.config.ApiKey)
		req.Header.Set("Api-Secret", p.config.ApiSecret)

		resp, err := p.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusCreated {
			return errors.New("put (insert): unexpected HTTP status: " + resp.Status)
		}

		(*item).SetVersion(itemVersion + 1)
		return nil
	}

	filter := map[string]interface{}{
		"id":      (*item).GetId(),
		"version": (*item).GetVersion(),
	}

	// Use patch endpoint to update the document. The filter identifies id and actual version.
	patchQuery := map[string]interface{}{
		"filter": filter,
		"patch":  item,
	}
	(*item).SetVersion(itemVersion + 1)
	payload, err := json.Marshal(patchQuery)
	(*item).SetVersion(itemVersion)
	if err != nil {
		return err
	}
	endpoint := p.config.Base + "/collections/" + url.PathEscape(p.config.Collection) + ":patch"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Api-Key", p.config.ApiKey)
	req.Header.Set("Api-Secret", p.config.ApiSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	err = json.NewDecoder(resp.Body).Decode(&item)
	if err == io.EOF {
		return store.ErrVersionGone
	}
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("put (patch): unexpected HTTP status: " + resp.Status)
	}

	(*item).SetVersion(itemVersion + 1)
	return nil
}

func (p *StoreInception[T]) Get(ctx context.Context, id string) (*T, error) {
	query := FindQuery{
		Filter: map[string]interface{}{
			"id": id,
		},
		Limit: 1,
	}
	payload, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	endpoint := p.config.Base + "/collections/" + url.PathEscape(p.config.Collection) + ":find"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Api-Key", p.config.ApiKey)
	req.Header.Set("Api-Secret", p.config.ApiSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("get: unexpected HTTP status: " + resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)
	var item *T
	if err := decoder.Decode(&item); err != nil {
		if err == io.EOF {
			// Document not found
			return nil, nil
		}
		return nil, err
	}
	return item, nil
}

func (p *StoreInception[T]) Delete(ctx context.Context, id string) error {
	query := FindQuery{
		Filter: map[string]interface{}{
			"id": id,
		},
	}
	payload, err := json.Marshal(query)
	if err != nil {
		return err
	}

	endpoint := p.config.Base + "/collections/" + url.PathEscape(p.config.Collection) + ":remove"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Api-Key", p.config.ApiKey)
	req.Header.Set("Api-Secret", p.config.ApiSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("delete: unexpected HTTP status: " + resp.Status)
	}
	return nil
}

func (p *StoreInception[T]) ensureCollection() error {
	endpoint := p.config.Base + "/collections"

	payload, err := json.Marshal(map[string]interface{}{
		"name": p.config.Collection,
	}) // todo: handle err
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Api-Key", p.config.ApiKey)
	req.Header.Set("Api-Secret", p.config.ApiSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusCreated {
		return nil
	}

	io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

func (p *StoreInception[T]) dropCollection() error {
	endpoint := p.config.Base + "/collections/" + url.PathEscape(p.config.Collection) + ":dropCollection"

	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Api-Key", p.config.ApiKey)
	req.Header.Set("Api-Secret", p.config.ApiSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	return nil
}
