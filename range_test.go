package main

import (
	"fmt"
	"testing"
)

func TestRange(t *testing.T) {

	r := Range(0, 100, 30)

	fmt.Print(r)

}
