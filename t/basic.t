use strict;
use warnings;

use Test::More;
use JSON::XS;
use Test::Exception;
use AnyEvent::Riak;
use YAML::Syck;

#plan tests => 15;

my ( $host, $path );

BEGIN {
    my $riak_test = $ENV{RIAK_TEST_SERVER};
    ( $host, $path ) = split ";", $riak_test if $riak_test;
    plan skip_all =>
        'set $ENV{RIAK_TEST_SERVER} if you want to run the tests'
        unless ( $host && $path );
}

ok my $riak = AnyEvent::Riak->new( host => $host, path => $path, w => 1,
    dw => 1),
    'create riak object';

# ping
ok my $ping_one = $riak->is_alive(
    sub {
        pass "is alive in cb";
	return $_[0];
    }
), 'ping with callback';

ok my $ping_two = $riak->is_alive()->recv, 'ping without callback';
is $ping_two, 'OK', 'valid response from ping without callback';

ok my $s = $ping_one->recv, 'response from ping without callback';
is $s, 'OK', 'valid response from ping';

# list bucket
ok my $bucket_cb = $riak->list_bucket(
    'bar',
    {},
    sub {
    	my $stuff = shift;
        my $res = JSON::decode_json($stuff);
        is scalar @{$res->{keys}}, 0, '0 keys in cb';
    }
    ),
    'fetch bucket list';

ok my $buckets = $riak->list_bucket('bar')->recv, "fetch bucket list";
is scalar @{ $buckets->{keys} }, '0', 'no keys';

ok my $res_bucket = $bucket_cb->recv, 'get bucket';

# set bucket
ok my $new_bucket
     = $riak->set_bucket( 'foo', { props => { n_val => 2 } } )->recv,
     'set a new bucket';

my $value = {
   foo => 'bar',
};

ok my $res = $riak->store('foo', 'bar', $value)->recv, 'set a new key';

ok $res = $riak->fetch( 'foo', 'bar' )->recv, 'fetch our new key';
is_deeply $res, $value, 'value is ok';
ok $res = $riak->delete( 'foo', 'bar' )->recv, 'delete our key';

ok my $store_w_cb = $riak->store(
    'foo', 'bar3', $value, undef, undef,
    sub {
        pass "store value ok";
        $riak->fetch(
            'foo', 'bar3', undef,
            sub {
                my $body = shift;
		is_deeply (JSON::decode_json($body), $value, 'value is ok in cb');
            }
        );
    }
);

ok my $final_res = $store_w_cb->recv;
$final_res->recv; # FIXME all cb should be called at this point

# ok $res = $riak->store($value)->recv, '... set a new key';
# my $second_value = {
#     bucket => 'foo',
#     key    => 'baz',
#     object => { foo => "bar", baz => 2 },
#     links  => [ [ 'foo', 'bar', 'tagged' ] ],
# };
# ok $res = $riak->store($second_value)->recv, '... set another new key';

# ok $res = $riak->walk( 'foo', 'baz', [ { bucket => 'foo', } ] )->recv,
#     '... walk';
# is $res->{results}->[0]->[0]->{key}, "bar", "... walked to bar";

done_testing();
