#!/usr/bin/env perl

# Perl version
# M. Goddard, 04/26/2013

use strict;

while (<>)
{
  chomp;
  next if /^CUSTOMER ID/; # Skip header
  my ($cust_id, $order_id, $gift_card, @receipt_items) = split /\|/;
  $gift_card =~ s!n/a!0!i;
  $gift_card =~ s/\$//;
  foreach my $item (@receipt_items)
  {
    $item =~ s/;/|/g;
    print join('|', $cust_id, $order_id, $gift_card, $item) . "\n";
  }
}

