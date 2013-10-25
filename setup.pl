use strict;
use warnings;

foreach my $ip (@ARGV){
    my $pid=fork();
    die "Cannot fork: $!" if (! defined $pid);
    if (! $pid) {
	`ssh  -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu\@10.0.0.$ip "sudo apt-get update; sudo apt-get -y install openjdk-6-jre-headless"`;
	`tar zcf - hadoop .ssh | ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu\@10.0.0.$ip "tar zxf -"`;
	exit;
    }
}


