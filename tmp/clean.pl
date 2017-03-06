#!/usr/bin/perl
$date="2017-03-04";
$file="/Users/kent/tmp/akka_test/log/$date/order.out";
$result="/Users/kent/tmp/akka_test/log/$date/order.result";
open(FH,'<',$file) or die("no such file");
open(FR,'>',$result) or die("no such file");
while(<FH>){
    chomp $_;
    @cols = split(/-/,$_);
    $str = join(",",@cols);
    print FR $date.",".$str."\n";
}
close FH;
close FR;
