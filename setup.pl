use strict;
use warnings;

foreach my $ip (@ARGV){
    my $pid=fork();
    die "Cannot fork: $!" if (! defined $pid);
    if (! $pid) {
	print $ip;
	`ssh  -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu\@10.0.0.$ip "sudo apt-get update; sudo apt-get -y install openjdk-6-jre-headless"`;
	print $ip;
	`tar zcf - hadoop .ssh | ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu\@10.0.0.$ip "tar zxf -"`;
	print $ip;
	`ssh  -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu\@10.0.0.$ip "rm -rf name data tmp mapred"`;
	exit;
    }
}


