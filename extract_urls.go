package main

import (
	"regexp"
	"strings"

	"github.com/antchfx/htmlquery"
)

var regex, _ = regexp.Compile("[^a-zA-Z]+")

func ExtractUrls(url string) (Files, error) {
	files := []File{}
	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}

	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return Files{files}, err
	}

	links := htmlquery.Find(doc, "//tr/td[2]/a")
	dates := htmlquery.Find(doc, "//tr/td[3]")

	for i, link := range links {
		name := htmlquery.InnerText(link)

		if strings.HasSuffix(name, ".zip") {
			type_file := regex.ReplaceAllString(strings.Split(name, ".")[0], "")

			u := url + name
			d := htmlquery.InnerText(dates[i])
			files = append(files, File{Url: u, UpdatedAtStr: d, Type: type_file})
		}
	}

	return Files{files}, nil
}
