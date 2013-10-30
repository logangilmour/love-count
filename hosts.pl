use strict;
use warnings;

open(my $file,'>',"/tmp/hosts") or die("Couldn't write to hosts");
open(my $slaves,'>',"hadoop/conf/slaves") or die("Couldn't write to slaves");
print $file "127.0.0.1 localhost\n\n";

my $me = `cat /etc/hostname`;
chomp $me;
foreach my $ip(@ARGV){
    my $command = "ssh ubuntu\@10.0.0.$ip -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no 'cat /etc/hostname'";
    my $name = `$command`;
    chomp $name;
    if ($me ne $name){ print $slaves $name."\n";}
    print $file "10.0.0.$ip $name.novalocal $name\n";
}
close $slaves;
close $file;
foreach my $ip (@ARGV){
    `cat /tmp/hosts | ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu\@10.0.0.$ip "cat > /tmp/hosts; sudo mv /tmp/hosts /etc/hosts"`;
}

