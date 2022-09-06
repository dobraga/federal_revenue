resource "google_bigquery_dataset" "default" {
  dataset_id = local.envs["DATASET_NAME"]
}

resource "google_bigquery_table" "create_table_Bookmark" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Bookmark"
  schema     = file("../pipe/resources/Bookmark.json")
}

resource "google_bigquery_table" "create_table_Cnaes" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Cnaes"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CODIGO"]
  schema     = file("../pipe/resources/Cnaes.json")
}

resource "google_bigquery_table" "create_table_Empresas" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Empresas"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CNPJ_BASICO"]
  schema     = file("../pipe/resources/Empresas.json")
}

resource "google_bigquery_table" "create_table_Estabelecimentos" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Estabelecimentos"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CNPJ_BASICO"]
  schema     = file("../pipe/resources/Estabelecimentos.json")
}

resource "google_bigquery_table" "create_table_Motivos" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Motivos"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CODIGO"]
  schema     = file("../pipe/resources/Motivos.json")
}

resource "google_bigquery_table" "create_table_Municipios" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Municipios"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CODIGO"]
  schema     = file("../pipe/resources/Municipios.json")
}

resource "google_bigquery_table" "create_table_Naturezas" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Naturezas"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CODIGO"]
  schema     = file("../pipe/resources/Naturezas.json")
}

resource "google_bigquery_table" "create_table_Paises" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Paises"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CODIGO"]
  schema     = file("../pipe/resources/Paises.json")
}

resource "google_bigquery_table" "create_table_Qualificacoes" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Qualificacoes"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CODIGO"]
  schema     = file("../pipe/resources/Qualificacoes.json")
}

resource "google_bigquery_table" "create_table_Simples" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Simples"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CNPJ_BASICO"]
  schema     = file("../pipe/resources/Simples.json")
}

resource "google_bigquery_table" "create_table_Socios" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "Socios"
  time_partitioning {
    type  = "DAY"
    field = "CREATED_AT"
  }
  clustering = ["CNPJ_BASICO"]
  schema     = file("../pipe/resources/Socios.json")
}
