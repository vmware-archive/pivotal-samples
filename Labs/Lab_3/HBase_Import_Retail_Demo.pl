#!/usr/bin/env perl

# $Id: HBase_Import_Retail_Demo.pl,v 1.7 2013/04/23 13:27:49 gpadmin Exp gpadmin $
#
# Takes ~ 10 minutes to run
#
# You can clean up these tables using the hbase shell:
#  hbase(main):002:0> disable_all '^.+?_dim$'
#  hbase(main):003:0> drop_all '^.+?_dim$'

use strict;

#
# To get past the issue:
#
#   java.lang.NoClassDefFoundError: com/google/common/base/Function,
#
# I had to add one JAR to HADOOP_CLASSPATH in hadoop-env.sh:
#   $HBASE_ROOT/lib/guava-r06.jar (Value of `LIB_JARS', below)
#

my $HBASE_ROOT = "/usr/lib/gphd/hbase";
my $HBASE_JAR = "$HBASE_ROOT/hbase-0.94.2-gphd-2.0.1.0-SNAPSHOT.jar";
my $LIB_JARS = "$HBASE_ROOT/lib/guava-11.0.2.jar";
my $DATA_DIR = "/retail_demo";

my %table_to_col = (
  # ROWKEY: customer_address_id
  # Import took 6:54
  customer_addresses_dim => [qw(
    customer_id
    valid_from_timestamp
    valid_to_timestamp
    house_number
    street_name
    appt_suite_no
    city
    state_code
    zip_code
    zip_plus_four
    country
    phone_number
  )],
  # ROWKEY: customer_id
  customers_dim => [qw(
    first_name
    last_name
    gender
  )],
  # ROWKEY: customer_id
  email_addresses_dim => [qw(
    email_address
  )],
  # ROWKEY: product_id
  products_dim => [qw(
    category_id
    price
    product_name
  )],
);

foreach my $table (keys %table_to_col)
{
  #create_hbase_table($table);
  import_to_hbase($table);
}

sub create_hbase_table {
  my $t_name = shift;
  print "Creating HBase table '$t_name' ...\n";
  open HBASE, "| hbase shell" or die $!;
  print HBASE "create '$t_name', 'cf1'\n";
  close HBASE;
}

sub import_to_hbase {
  my $t_name = shift;
  print "Running the 'importtsv' map/reduce job to load '$t_name' into HBase ...\n";
  my $cols = join(",", map { "cf1:$_" } @{$table_to_col{$t_name}});
  my $cmd = "hadoop jar $HBASE_JAR importtsv -libjars $LIB_JARS";
  $cmd .= " -Dimporttsv.columns=HBASE_ROW_KEY,$cols";
  $cmd .= " $t_name $DATA_DIR/$t_name";
  print "\n-------\n";
  print $cmd;
  print "\n";
#  system $cmd;
}

