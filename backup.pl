#!/usr/bin/env perl

# -------------------------------------------
# Install instructions on debian:
#
# apt-get install libconfig-auto-perl libmime-lite-perl rsync libauthen-sasl-perl libmime-base64-urlsafe-perl pigz
#
#

use strict;
use warnings;
use v5.22.1;

no warnings "experimental::lexical_subs";
use feature 'lexical_subs';

use Config::Auto;
use MIME::Lite;
use POSIX;

use constant SSH_OPTS => '-q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no';
use constant LOG_FILE => '/var/log/backuper.log';
use constant ROTATE_PERIODS => qw(day week month year);
use constant ROTATE_ALL => -1;

my sub trim;
my sub is_abs;
my sub alert;
my sub begins_with;
my sub log_info;

our $common_dest_folder;
our $common_rotate_folder;
our $common_ssh_user;
our $common_db_user;

our $email_use;
our $email_dest;
our $email_sender;
our $email_smtp_server;
our $email_smtp_user;
our $email_smtp_pwd;

sub load_conf {
    eval { 
        my $ca = Config::Auto->new(format => 'yaml');
        my $conf = $ca->parse();

        my %rotate_conf;
        die "No rotate configuration is provided" unless defined($conf->{'rotate'});
        foreach my $period (ROTATE_PERIODS) {
            my $value = defined($conf->{'rotate'}->{$period}) ? $conf->{'rotate'}->{$period} : "all";
            if($value =~ /^[0-9]+$/) {
                $value = int($value);
            }
            elsif($value eq "all") {
                $value = ROTATE_ALL;
            }
            else {
                die "Unknown rotate value '".$value."' for ".$period;
            }
            $rotate_conf{$period} = $value;
        }

        die "No email configuration is provided" unless defined($conf->{'email'});
        die "Missing use_email value" unless defined($conf->{'email'}->{'use_email'});
        if($conf->{'email'}->{'use_email'} =~ /(1|on|ok|active|yes|true)/i) {
            $email_use = 1;
            $email_dest = $conf->{'email'}->{'sysadmin_email'};
            $email_sender = $conf->{'email'}->{'backup_email'};
            $email_smtp_user = $conf->{'email'}->{'smtp_user'};
            $email_smtp_pwd = $conf->{'email'}->{'smtp_pwd'};
            $email_smtp_server = $conf->{'email'}->{'smtp_server'};
        }
        else {
            $email_use = 0;
        }

        die "No common configuration is provided" unless defined($conf->{'common'});
        die "No dest folder configuration is provided" unless defined($conf->{'common'}->{'dest_folder'});
        our $common_dest_folder = $conf->{'common'}->{'dest_folder'};
        die "No rotate folder configuration is provided" unless defined($conf->{'common'}->{'rotate_folder'});
        our $common_rotate_folder = $conf->{'common'}->{'rotate_folder'};
        die "No ssh user configuration is provided" unless defined($conf->{'common'}->{'ssh_user'});
        our $common_ssh_user = $conf->{'common'}->{'ssh_user'};
        die "No db user configuration is provided" unless defined($conf->{'common'}->{'db_user'});
        our $common_db_user = $conf->{'common'}->{'db_user'};

        undef $conf->{'email'};
        undef $conf->{'rotate'};
        undef $conf->{'common'};

        my %result = ('servers' => $conf, 'rotate' => \%rotate_conf);
        return \%result;
        1;
    }
    or do {
        my $msg = "Bad configuration file";
        $msg .= "\n".$! if defined($!) and length($!);
        $msg .= "\n".$@ if defined($@) and length($@);
        open(my $log_fh, ">>", LOG_FILE);
        printf $log_fh "[%12s] ERROR: ", time;
        print $log_fh $msg;
        print $log_fh "\n";
        close($log_fh);
        die $msg;
    };
}

sub main {
    $ENV{'LC_ALL'} = 'en_US.UTF-8';
    
    log_info "Starting backup";

    my $result = load_conf();
    my $servers_conf = $result->{'servers'};
    my $rotate_conf = $result->{'rotate'};

    eval {
        check_free_space();
        1;
    }
    or do {
        alert "Unable to check available disk space";
    };

    eval {
        rotate_old_backups($rotate_conf);
        1;
    }
    or do {
        return alert "Unable to rotate logs";
    };

    eval {
        backup($servers_conf);
        1;        
    }
    or do {
        return alert "Unable to do all backups";
    };

    log_info "Backup finished successfully";
}

sub check_free_space {
    my $cmd = "df -m --output=avail '$common_dest_folder' | tail -n1";
    my $space = qx/$cmd/;
    chomp($space);
    $space = int($space/1024); # convert into GB
    alert "only $space GigaBytes availables for backups" if $space < 50; 
}

sub elapsed_since {
    my ($now, $since) = @_;
    my @time = gmtime($now);
    
    my $nbr_sec_since_day = ($time[2] * 3600) + ($time[1] * 60) + $time[0];
    return $nbr_sec_since_day if $since eq 'day';
    
    if($since eq 'week') {
        my $dow_sec = int(POSIX::strftime("%u", @time))*(24*3600);
        return $nbr_sec_since_day if $dow_sec <= 24*3600;
        return $dow_sec + $nbr_sec_since_day - (24*3600);
    }
    return $time[3]*(24*3600) + $nbr_sec_since_day if $since eq 'month';
    return int(POSIX::strftime("%j", @time))*(24*3600) + $nbr_sec_since_day if $since eq 'year';
    die "Unknown time reference $since";
}

sub rotate_old_backups {
    my ($conf) = @_;
    
    # Generate the valid dates of archives we shouln't destroy
    my $now = time();
    my @keep = ();
    my $nbr_days = $conf->{'day'} == ROTATE_ALL ? 7 : $conf->{'day'};
    for(my $i = 0; $i < $nbr_days; $i++) {
        push @keep, POSIX::strftime("%Y%m%d", gmtime($now-elapsed_since($now, 'day')-($i*24*3600)));
    }
    my $nbr_weeks = $conf->{'week'} == ROTATE_ALL ? 4 : $conf->{'week'};
    for(my $i = 0; $i < $nbr_weeks; $i++) {
        push @keep, POSIX::strftime("%Y%m%d", gmtime($now-(86400*$i*7)-elapsed_since($now, 'week')+1));
    }
    my $nbr_month = $conf->{'month'} == ROTATE_ALL ? 12 : $conf->{'month'};
    my $this_year = int(POSIX::strftime("%Y", gmtime));
    my $this_month = int(POSIX::strftime("%m", gmtime));
    for(my $i = 0; $i < $nbr_month; $i++) {
        my $nbr_monthes = $this_year*12 + ($this_month - $i); 
        push @keep, sprintf("%04d%02d01", int($nbr_monthes/12), $nbr_monthes %12);
    } 
    if($conf->{'year'} != ROTATE_ALL) {
        for(my $i = 0; $i < $conf->{'year'}; $i++) {
            push @keep, ($this_year - $i)."0101";
        }
    }

    # Check if we should remove the archive
    opendir(my $past_folder, $common_rotate_folder) or die "Unable to read folder $common_rotate_folder";
    while(my $file = readdir($past_folder)) {
        next if ($file =~ /^..?$/);  # skip . and ..
        next unless -f "$common_rotate_folder/$file";
        next unless $file =~ /^([0-9]{8})_.*$/;

        my $date = $1;
        next if grep { $_ == $date } @keep;
        next if $date =~ /0101$/ and $conf->{'year'} == ROTATE_ALL;

        log_info "removing old backup $file";
        unlink "$common_rotate_folder/$file";
    }
    close($past_folder);
    
    my $today = POSIX::strftime("%Y%m%d", gmtime($now));

    # move current backup to new archive
    opendir(my $dest_folder, $common_dest_folder) or die "Unable to read folder $common_dest_folder";
    while(my $file = readdir($dest_folder)) {
        next if ($file =~ /^..?$/);  # skip . and ..
        
        my $dest = "$common_rotate_folder/${today}_$file";
        $dest .= ".tgz" if -d "$common_dest_folder/$file";
        
        if(-f "$common_dest_folder/$file") {
            unlink $dest if -e $dest;
            my $cmd = "mv '$common_dest_folder/$file' '$dest' 2>&1";
            my $output = qx/$cmd/;
            die "copy failed: ".$output if $? != 0;
        }
        elsif(-d "$common_dest_folder/$file") {
            unlink $dest if -e $dest;
            my $cmd = "nice -2 tar -c --use-compress-program=pigz -C '$common_dest_folder' -f '$dest' '$file' 2>&1";
            my $output = qx/$cmd/;
            die "copy failed: ".$output if $? != 0;
        }
        else {
            die "don't known what to do with '$common_dest_folder/$file'";
        }
    }
    close($dest_folder);
}

sub backup {
    my ($conf) = @_;

    foreach my $server (keys %{$conf}) {
        my $prefix = defined($conf->{$server}->{'prefix'}) ? $conf->{$server}->{'prefix'} : '';

        if(defined($conf->{$server}->{'files'})) {
            my @excluded = ();
            if(defined($conf->{$server}->{'files'}->{'exclude'})) {
                @excluded = @{$conf->{$server}->{'files'}->{'exclude'}};
            }
            foreach my $archive (keys %{$conf->{$server}->{'files'}}) {
                next if $archive eq 'exclude';
                eval {
                    my $src_path = $conf->{$server}->{'files'}->{$archive};
                    my @local_excluded = grep { begins_with($_, $src_path) } @excluded;
                    backup_files($server, $prefix, $archive, $src_path, \@local_excluded);
                    1;
                }
                or do {
                    alert "Unable to backup '$archive' folder of server $server";
                };
            }
        }

        if(defined($conf->{$server}->{'databases'})) {
            foreach my $archive (keys %{$conf->{$server}->{'databases'}}) {
                eval {
                    my ($db_engine, $port, $db_name) = split(/:/, $conf->{$server}->{'databases'}->{$archive});
                    backup_database($server, $prefix, $archive, $db_engine, $port, $db_name);
                    1;
                }
                or do {
                    alert "Unable to backup '$archive' database of server $server";
                };
            }
        }
    }
}

sub backup_files {
    my ($server, $prefix, $archive, $path, $excluded) = @_;
    
    log_info "starting backup of folder $path on $server";    
    $path =~ s/\/*$//;
    my $cmd = "rsync -e 'ssh ".SSH_OPTS."' --delete -a ";
    foreach my $exclusion (@{$excluded}) {
        $cmd .= " --exclude='".substr($exclusion, length($path))."' ";
    }
    $cmd .= " '$common_ssh_user\@$server:/$path/' '$common_dest_folder/$prefix$archive/' 2>&1";
    my $output = qx/$cmd/;
    $output = "" unless defined($output);
    die "Rsync failed: ".$output if $? != 0;
    my $touch_cmd = "touch '$common_dest_folder/$prefix$archive/.backup_date'";
    qx/$touch_cmd/;
    log_info "folder $path on $server successfully backuped";
}

sub backup_database {
    my ($server, $prefix, $archive, $db_engine, $port, $db_name) = @_;

    log_info "starting backup of db $db_name on $server";
    if($db_engine eq "mysql") {
        my $cmd = "ssh ".SSH_OPTS." '$common_ssh_user\@$server' 'mysqldump -u \"$common_db_user\" ";
        $cmd .= " -h localhost \"--port=$port\" --databases \"$db_name\" | gzip'";
        $cmd .= " 2>&1 1>'$common_dest_folder/$prefix$archive.sql.gz'";
        my $output = qx/$cmd/;
        $output = "" unless defined($output);
        die "Database backup failed: ".$output if $? != 0;
        log_info "db $db_name on $server successfully backuped";
    }
    elsif($db_engine eq "mongo") {
        my $cmd = "ssh ".SSH_OPTS." '$common_ssh_user\@$server' 'mongodump --archive --gzip ";
        $cmd .= " --host localhost \"--port=$port\" --db \"$db_name\"'";
        $cmd .= " 2>&1 1>'$common_dest_folder/$prefix$archive.mongo.gz'";
        my $output = qx/$cmd/;
        $output = "" unless defined($output);
        die "Database backup failed: ".$output if $? != 0;
        log_info "db $db_name on $server successfully backuped";
    }
    elsif($db_engine eq "postgres") {
        my $cmd = "ssh ".SSH_OPTS." '$common_ssh_user\@$server' 'pg_dump ";
        $cmd .= " -h localhost -p \"$port\" -d \"$db_name\" | gzip '";
        $cmd .= " 2>&1 1>'$common_dest_folder/$prefix$archive.sql.gz'";
        my $output = qx/$cmd/;
        $output = "" unless defined($output);
        die "Database backup failed: ".$output if $? != 0;
        log_info "db $db_name on $server successfully backuped";        
    }
    else {
        die "unknown database engine '$db_engine'";
    }
}

sub alert {
    my $msg = join("\n", grep({ defined($_) } @_));

    $msg .= "\n".$! if defined($!) and length($!);
    $msg .= "\n".$@ if defined($@) and length($@);

    open(my $log_fh, ">>", LOG_FILE);
    printf $log_fh "[%12s] ERROR: ", time;
    print $log_fh $msg;
    print $log_fh "\n";
    close($log_fh);

    return 1 unless $email_use;
    my $mail = MIME::Lite->new(
        From    => $email_sender,
        To      => $email_dest,
        Subject => "Backup Error",
        Data    => $msg
    );
    $mail->send('smtp', $email_smtp_server, AuthUser=>$email_smtp_user, AuthPass=>$email_smtp_pwd);
    return 1;
}

sub log_info {
    my $msg = join("\n", grep({ defined($_) } @_)); 
    
    open(my $log_fh, ">>", LOG_FILE);
    printf $log_fh "[%12s] INFO: ", time;
    print $log_fh $msg;
    print $log_fh "\n";
    close($log_fh);
}

sub trim {
    my @out = grep { defined($_) } @_;
    for(@out) {
        s/^\s+//;
        s/\s+$//;
    }
    return wantarray ? @out : $out[0];
}

sub is_abs {
    my ($filepath) = @_;

    if($^O =~ /Win/ ){  # Windows
        return $filepath =~ /^[A-Z]:\\/;  
    }
    else { # we suppose *nix oses
        return $filepath =~ /^\//;
    }
}

sub begins_with {
    my ($haystack, $needle) = @_;
    return substr($haystack, 0, length($needle)) eq $needle;
}

main();

__END__

