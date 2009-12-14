use strict;
use warnings;
use Test::More;
use JSON::XS;
use Test::Exception;
use AnyEvent::Riak;

my $jiak = AnyEvent::Riak->new(
    host => 'http://127.0.0.1:8098',

    #host => 'http://192.168.0.11:8098',
    path => 'jiak'
);

ok my $buckets = $jiak->list_bucket('bar')->recv, "... fetch bucket list";
is scalar @{ $buckets->{keys} }, '0', '... no keys';

ok my $new_bucket
    = $jiak->set_bucket( 'foo', { allowed_fields => '*' } )->recv,
    '... set a new bucket';

my $value = {
    bucket => 'foo',
    key    => 'bar',
    object => { foo => "bar", baz => 1 },
    links  => []
};

ok my $res = $jiak->store($value)->recv, '... set a new key';

ok $res = $jiak->fetch( 'foo', 'bar' )->recv, '... fetch our new key';
ok $res = $jiak->delete( 'foo', 'bar' )->recv, '... delete our key';

dies_ok { $jiak->fetch( 'foo', 'foo' )->recv } '... dies when error';
like $@, qr/404/, '... 404 response';

ok $res = $jiak->store($value)->recv, '... set a new key';
my $second_value = {
    bucket => 'foo',
    key    => 'baz',
    object => { foo => "bar", baz => 2 },
    links  => [ [ 'foo', 'bar', 'tagged' ] ],
};
ok $res = $jiak->store($second_value)->recv, '... set another new key';

ok $res = $jiak->walk( 'foo', 'baz', [ { bucket => 'foo', } ] )->recv,
    '... walk';
is $res->{results}->[0]->[0]->{key}, "bar", "... walked to bar";

done_testing();
