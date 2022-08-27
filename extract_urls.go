package main

import (
	"strings"

	"github.com/antchfx/htmlquery"
)

func ExtractUrls(url string) ([]string, error) {
	urls := []string{}
	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}

	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return urls, err
	}

	links := htmlquery.Find(doc, "//a")

	for _, link := range links {
		name := htmlquery.InnerText(link)
		if strings.HasSuffix(name, ".zip") {
			urls = append(urls, url+name)
		}
	}

	return urls, nil
}
