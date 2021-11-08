




run_setup <-
  function(civic_schema = "civic",
           log_table_name = "setup_civic_log",
           civic_version = as.character(Sys.time()),
           verbose = TRUE,
           render_sql = TRUE) {
# Download files
dl_links <-
  list(
    GENE = "https://civicdb.org/downloads/nightly/nightly-GeneSummaries.tsv",
    VARIANT = "https://civicdb.org/downloads/nightly/nightly-VariantSummaries.tsv",
    EVIDENCE = "https://civicdb.org/downloads/nightly/nightly-ClinicalEvidenceSummaries.tsv",
    VARIANT_GROUP = "https://civicdb.org/downloads/nightly/nightly-VariantGroupSummaries.tsv",
    ASSERTION = "https://civicdb.org/downloads/nightly/nightly-AssertionSummaries.tsv")


ddls <-
  list(
    GENE =
      "
      DROP TABLE IF EXISTS {civic_schema}.gene;
      CREATE TABLE {civic_schema}.gene (
    gene_id integer,
    gene_civic_url character varying(255),
    name character varying(255),
    entrez_id integer,
    description text,
    last_review_date character varying(255),
    is_flagged character varying(255)
);",
    VARIANT =
      "
    DROP TABLE IF EXISTS {civic_schema}.variant;
    CREATE TABLE {civic_schema}.variant (
    variant_id integer,
    variant_civic_url character varying(255),
    gene character varying(255),
    entrez_id integer,
    variant character varying(255),
    summary text,
    variant_groups character varying(255),
    chromosome character varying(255),
    start integer,
    stop integer,
    reference_bases character varying(255),
    variant_bases character varying(255),
    representative_transcript character varying(255),
    ensembl_version integer,
    reference_build character varying(255),
    chromosome2 character varying(255),
    start2 integer,
    stop2 integer,
    representative_transcript2 character varying(255),
    variant_types character varying(255),
    hgvs_expressions character varying(255),
    last_review_date character varying(255),
    civic_variant_evidence_score numeric,
    allele_registry_id character varying(255),
    clinvar_ids character varying(255),
    variant_aliases character varying(255),
    assertion_ids text,
    assertion_civic_urls character varying(255),
    is_flagged character varying(255)
);
    ",
    EVIDENCE =
      "
    DROP TABLE IF EXISTS {civic_schema}.evidence;
    CREATE TABLE {civic_schema}.evidence (
    gene character varying(255),
    entrez_id integer,
    variant character varying(255),
    disease character varying(255),
    doid character varying(255),
    phenotypes character varying(255),
    drugs character varying(255),
    drug_interaction_type character varying(255),
    evidence_type character varying(255),
    evidence_direction character varying(255),
    evidence_level character varying(255),
    clinical_significance character varying(255),
    evidence_statement text,
    citation_id integer,
    source_type character varying(255),
    asco_abstract_id character varying(255),
    citation character varying(255),
    nct_ids character varying(255),
    rating integer,
    evidence_status character varying(255),
    evidence_id integer,
    variant_id integer,
    gene_id integer,
    chromosome character varying(255),
    start integer,
    stop integer,
    reference_bases character varying(255),
    variant_bases character varying(255),
    representative_transcript character varying(255),
    chromosome2 varchar(10),
    start2 integer,
    stop2 integer,
    representative_transcript2 character varying(255),
    ensembl_version integer,
    reference_build character varying(255),
    variant_summary text,
    variant_origin character varying(255),
    last_review_date character varying(255),
    evidence_civic_url character varying(255),
    variant_civic_url character varying(255),
    gene_civic_url character varying(255),
    is_flagged character varying(255)
);
    ",
    VARIANT_GROUP =
      "
    DROP TABLE IF EXISTS {civic_schema}.variant_group;
    CREATE TABLE {civic_schema}.variant_group (
    variant_group_id integer,
    variant_group_civic_url character varying(255),
    variant_group character varying(255),
    description text,
    last_review_date character varying(255),
    is_flagged character varying(255)
);
    ",
    ASSERTION =
      "
    DROP TABLE IF EXISTS {civic_schema}.assertion;
    CREATE TABLE {civic_schema}.assertion (
    gene character varying(255),
    entrez_id integer,
    variant character varying(255),
    disease character varying(255),
    doid character varying(255),
    phenotypes character varying(312),
    drugs character varying(255),
    assertion_type character varying(255),
    assertion_direction character varying(255),
    clinical_significance character varying(255),
    acmg_codes character varying(255),
    amp_category character varying(255),
    nccn_guideline character varying(255),
    nccn_guideline_version varchar(10),
    regulatory_approval character varying(255),
    fda_companion_test character varying(255),
    assertion_summary character varying(255),
    assertion_description character varying(1376),
    assertion_id integer,
    evidence_item_ids text,
    variant_id integer,
    gene_id integer,
    last_review_date character varying(255),
    assertion_civic_url character varying(255),
    evidence_items_civic_url character varying(1024),
    variant_civic_url character varying(255),
    gene_civic_url character varying(255),
    is_flagged character varying(255)
);
    "
  )


for (i in seq_along(dl_links)) {

  dl_link <- dl_links[[i]]
  dest_table <- names(dl_links)[i]
  ddl <-  glue::glue(ddls[[dest_table]])

  pg13::send(conn_fun = "pg13::local_connect()",
             sql_statement = ddl,
             verbose = verbose,
             render_sql = render_sql)

  tmp_tsv <- tempfile(fileext = ".tsv")
  download.file(dl_link,
                destfile = tmp_tsv)
  Sys.sleep(3)
  x <- readr::read_tsv(file = tmp_tsv,
                       col_types = readr::cols(.default = "c"))

  tmp_csv <- tempfile(fileext = ".csv")
  readr::write_csv(x = x,
                   file = tmp_csv,
                   quote = "all")


  pg13::send(
    conn_fun = "pg13::local_connect()",
    sql_statement =
      glue::glue("COPY {civic_schema}.{dest_table} FROM '{tmp_csv}' NULL AS 'NA' CSV HEADER QUOTE E'\"';")
  )

  unlink(tmp_tsv)
  unlink(tmp_csv)

}

civic_tables <-
  pg13::ls_tables(conn_fun = "pg13::local_connect()",
                  schema = "civic")


row_counts <-
  civic_tables %>%
  purrr::map(function(x) pg13::query(conn_fun = "pg13::local_connect()",
                              sql_statement = pg13::renderRowCount(schema = "civic",
                                                                   tableName = x))) %>%
  purrr::map(unlist) %>%
  purrr::map(unname) %>%
  purrr::set_names(civic_tables) %>%
  tibble::enframe(name = "civic_table",
          value = "row_count") %>%
  dplyr::mutate(row_count = unlist(row_count)) %>%
  tidyr::pivot_wider(names_from = civic_table,
              values_from = row_count)


colnames(row_counts) <-
sprintf("%s_ROWS", colnames(row_counts))

log_df <-
  cbind(
    tibble::tibble(
      sc_datetime = as.character(Sys.time()),
      civic_version = civic_version,
      civic_schema = civic_schema,
    ),
    row_counts
  )


if (pg13::table_exists(conn_fun = "pg13::local_connect()",
                       schema = "public",
                       table_name = log_table_name)) {

  row_values <- glue::glue_collapse(glue::single_quote(unname(unlist(log_df))), sep = ",")
  sql_statement <-
    glue::glue("INSERT INTO public.{log_table_name} VALUES ({row_values});")

  pg13::send(conn_fun = "pg13::local_connect()",
             sql_statement = sql_statement,
             verbose = verbose,
             render_sql = render_sql)

} else {

  pg13::write_table(conn_fun = "pg13::local_connect()",
                    schema = "public",
                    table_name = log_table_name,
                    data = log_df)


}
}
