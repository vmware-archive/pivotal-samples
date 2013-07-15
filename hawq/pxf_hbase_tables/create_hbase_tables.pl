#!/usr/bin/env perl

my %tables = (
 "customers_dim",       
 "categories_dim",        
 "customer_addresses_dim",
 "email_addresses_dim",
 "order_lineitems",
 "orders",
 "payment_methods",
 "products_dim"
 );


foreach my $table (%tables){
  print "$table\n";
  print "Creating HBase table '$table' ...\n";
  open HBASE, "| hbase shell" or die $!;
  print HBASE "create '$table', 'cf1'\n";
  close HBASE;
}
