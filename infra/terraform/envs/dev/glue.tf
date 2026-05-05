resource "aws_glue_catalog_database" "bronze" {
  name = "${local.db_prefix}_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${local.db_prefix}_silver"
}

resource "aws_glue_catalog_database" "gold" {
  name = "${local.db_prefix}_gold"
}

resource "aws_glue_catalog_database" "control" {
  name = "${local.db_prefix}_control"
}