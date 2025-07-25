package client

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Client struct {
	BaseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		http:    &http.Client{},
	}
}

func (c *Client) CheckConnection() error {
	resp, err := c.http.Get(c.BaseURL + "/ping")
	if err != nil {
		return fmt.Errorf("connection error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed: %s", resp.Status)
	}
	return nil
}

func (c *Client) Set(key, value string) error {
	escapedKey := url.PathEscape(key)
	escapedValue := url.PathEscape(value)
	url := fmt.Sprintf("%s/client/%s/%s", c.BaseURL, escapedKey, escapedValue)

	resp, err := c.http.Post(url, "text/plain", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set failed: %s - %s", resp.Status, string(body))
	}
	return nil
}

func (c *Client) Get(key string) (string, error) {
	escapedKey := url.PathEscape(key)
	url := fmt.Sprintf("%s/client/%s", c.BaseURL, escapedKey)

	resp, err := c.http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get failed: %s - %s", resp.Status, string(body))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *Client) Delete(key string) error {
	escapedKey := url.PathEscape(key)
	url := fmt.Sprintf("%s/client/%s", c.BaseURL, escapedKey)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed: %s - %s", resp.Status, string(body))
	}
	return nil
}

// AddNode sends a request to a controller to add a new node.
func (c *Client) AddNode(nodeID string) error {
	// Management tasks should go to a controller, not the load balancer.
	controllerURL := "http://controller:8080/node/add"

	// Correctly initialize url.Values and send the PostForm request.
	resp, err := c.http.PostForm(controllerURL, url.Values{"NodeID": {nodeID}})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add node failed: %s - %s", resp.Status, string(body))
	}
	return nil
}
