package kibana

import (
	"net/http"

	"github.com/zefhemel/ax/pkg/backend/common"
)

type Client struct {
	URL        string
	AuthHeader string
	Index      string
}

func New(url, authHeader, index string) *Client {
	return &Client{
		URL:        url,
		AuthHeader: authHeader,
		Index:      index,
	}
}

func (client *Client) addHeaders(req *http.Request) {
	req.Header.Set("Authorization", client.AuthHeader)
	//fmt.Println("Kibana version", client.KbnVersion)
	req.Header.Set("Kbn-Version", "")
	req.Header.Set("Content-Type", "application/x-ldjson")
	req.Header.Set("Accept", "application/json, text/plain, */*")
}

var _ common.Client = &Client{}
