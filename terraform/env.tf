locals {
  envs = { for tuple in regexall("(.*)=(.*)", file("../pipe/.env")) : tuple[0] => sensitive(tuple[1]) }
}