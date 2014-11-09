riak_exporter
=============

Tool to dump or load data on a RIAK DB

Usage
=====

    > bundle install
    > bundle exec ruby riak_dump.rb --help
    Usage: riak_dump.rb [options]
        -v, --verbose                    Run verbosely
        -d, --dump                       Dump data from Riak
        -l, --load                       Load data into Riak
        -f, --file FILE                  File to load or dump the data from/to.
        -b, --bucket BUCKET              (Optional) Bucket name to load or dump.
        -c, --count COUNT                (Optional) Limit records loaded/dumped to this number.
        -h, --host HOST                  Hostname or IP to connect to, will use default port.

To dump to a file from a RIAK server running on 33.33.33.10

    > bundle exec ruby riak_dump.rb -d -f dump.json -h 33.33.33.10

To load in data and override the source bucket

    > be ruby riak_dump.rb -l -f dump.json -h 33.33.33.10 -b newbucketname