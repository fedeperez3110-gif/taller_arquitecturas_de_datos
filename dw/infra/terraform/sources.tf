# Fuentes operativas migradas desde local: MySQL -> RDS for MySQL, MongoDB -> DocumentDB.

# ------------------------------------------------------------------ RDS MySQL
resource "random_password" "mysql" {
  length  = 24
  special = false
}

resource "aws_db_subnet_group" "mysql" {
  name       = "${var.project}-mysql"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_db_instance" "mysql" {
  identifier              = "${var.project}-mysql"
  engine                  = "mysql"
  engine_version          = var.mysql_engine_version
  instance_class          = var.mysql_instance_class
  allocated_storage       = 20
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = "ecommerce"
  username                = "etl_admin"
  password                = random_password.mysql.result
  db_subnet_group_name    = aws_db_subnet_group.mysql.name
  vpc_security_group_ids  = [aws_security_group.mysql.id]
  publicly_accessible     = false
  multi_az                = false
  backup_retention_period = 0 # laboratorio: sin backups automáticos (crea y destruye más rápido)
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true
}

# ------------------------------------------------------------------ DocumentDB
resource "random_password" "docdb" {
  length  = 24
  special = false
}

resource "aws_docdb_subnet_group" "docdb" {
  name       = "${var.project}-docdb"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_docdb_cluster" "docdb" {
  cluster_identifier     = "${var.project}-docdb"
  engine                 = "docdb"
  engine_version         = var.docdb_engine_version
  master_username        = "etl_admin"
  master_password        = random_password.docdb.result
  db_subnet_group_name   = aws_docdb_subnet_group.docdb.name
  vpc_security_group_ids = [aws_security_group.docdb.id]
  storage_encrypted      = true
  skip_final_snapshot    = true
  deletion_protection    = false
  apply_immediately      = true
  # El grupo de parámetros por defecto deja TLS obligatorio.
}

resource "aws_docdb_cluster_instance" "docdb" {
  identifier         = "${var.project}-docdb-1"
  cluster_identifier = aws_docdb_cluster.docdb.id
  instance_class     = var.docdb_instance_class
  apply_immediately  = true
}
