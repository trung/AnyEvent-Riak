package AnyEvent::Riak;

use strict;
use warnings;
use Carp;
use URI;
use JSON;
use AnyEvent;
use AnyEvent::HTTP;
use MIME::Base64;
use YAML::Syck;

our $VERSION = '0.02';

sub new {
    my ( $class, %args ) = @_;

    my $host        = delete $args{host}        || 'http://127.0.0.1:8098';
    my $path        = delete $args{path}        || 'riak';
    my $mapred_path = delete $args{mapred_path} || 'mapred';
    my $r           = delete $args{r}           || 2;
    my $d           = delete $args{w}           || 2;
    my $dw          = delete $args{dw}          || 2;

    my $client_id
        = "perl_anyevent_riak_" . encode_base64( int( rand(10737411824) ), '' );

    bless {
        host        => $host,
        path        => $path,
        mapred_path => $mapred_path,
        client_id   => $client_id,
        r           => $r,
        d           => $d,
        dw          => $dw,
        %args,
    }, $class;
}

sub _build_uri {
    my ( $self, $path, $options ) = @_;
    my $uri = URI->new( $self->{host} );
    $uri->path( join( "/", @$path ) );
    $uri->query_form( $self->_build_query($options) );
    return $uri->as_string;
}

sub _build_headers {
    my ( $self, $options) = @_;
    my $headers = {};
    $headers = {
        'X-Riak-ClientId' => $self->{client_id},
        'Content-Type'    => 'application/json',
    };
    return $headers;
}

sub _build_query {
    my ($self, $options) = @_;
    my $valid_options = [qw/props keys returnbody/];
    my $query;
    foreach (@$valid_options) {
        $query->{$_} = $options->{$_} if exists $options->{$_}
    }
    $query;
}

sub _init_callback {
    my $self = shift;

    my ( $cv, $cb );
    if (@_) {
        $cv = pop if UNIVERSAL::isa( $_[-1], 'AnyEvent::CondVar' );
        $cb = pop if ref $_[-1] eq 'CODE';
    }

    $cv ||= AE::cv;
    return ( $cv, $cb );
}

sub default_cb {
    my ( $self, $options ) = @_;
    return sub {
        my $res = shift;
        return $res;
    };
}

sub is_alive {
    my $self = shift;

    my ( $cv, $cb ) = $self->_init_callback(@_);
    $cb = $self->default_cb() if !$cb;

    http_request(
        GET => $self->_build_uri( [qw/ping/] ),
        headers => $self->_build_headers(),
        sub {
            my ( $body, $headers ) = @_;
            if ( $headers->{Status} == 200 ) {
                $cv->send( $cb->(1) );
            }
            else {
                $cv->send( $cb->(0) );
            }
        },
    );
    return $cv;
}

sub list_bucket {
    my $self        = shift;
    my $bucket_name = shift;
    my $options     = shift;

    my ( $cv, $cb ) = $self->_init_callback(@_);
    $cb = $self->default_cb() if !$cb;

    http_request(
        GET => $self->_build_uri( [ $self->{path}, $bucket_name ], $options ),
        headers => $self->_build_headers(),
       sub {
           my ($body, $headers) = @_;
           if ($body && $headers->{Status} == 200) {
               my $res = JSON::decode_json($body);
               $cv->send($cb->($res));
           }else{
               $cv->send(undef);
           }
       }
    );
    return $cv;
}

sub set_bucket {
    my $self   = shift;
    my $bucket = shift;
    my $schema = shift;

    my ( $cv, $cb ) = $self->_init_callback(@_);
    $cb = $self->default_cb() if !$cb;

    http_request(
        PUT => $self->_build_uri( [ $self->{path}, 'bucket' ] ),
        headers => $self->_build_headers(),
        body    => JSON::encode_json($schema),
        sub {
            my ($body, $headers) = @_;
            if ($headers->{Status} == 204) {
                $cv->send($cb->(1));
            }else{
                $cv->send($cb->(0));
            }
        }
    );
    $cv;
}

sub fetch {
    my $self   = shift;
    my $bucket = shift;
    my $key    = shift;
    my $r      = shift;

    $r = $self->{r} if !$r;

    my ( $cv, $cb ) = $self->_init_callback(@_);
    $cb = $self->default_cb( { json => 1 } ) if !$cb;

    http_request(
        GET => $self->_build_uri( [ $self->{path}, $bucket, $key ] ),
        headers => $self->_build_headers(),
        sub {
            $cv->send( $cb->(@_) );
        }
    );
    $cv;
}

sub store {
    my $self   = shift;
    my $bucket = shift;
    my $key    = shift;
    my $data   = shift;
    my $w      = shift;
    my $dw     = shift;

    $w  = $self->{w}  if !$w;
    $dw = $self->{dw} if !$dw;

    my ( $cv, $cb ) = $self->_init_callback(@_);
    $cb = $self->default_cb( { json => 0 } ) if !$cb;

    my $json = JSON::encode_json($data);

    http_request(
        POST => $self->_build_uri( [ $self->{path}, $bucket, $key ] ),
        headers => $self->_build_headers(),
        body    => $json,
        sub {
            $cv->send( $cb->(@_) );
        }
    );
    $cv;
}

sub delete {
    my $self = shift;
    my $bucket = shift;
    my $key = shift;

    my ($cv, $cb) = $self->_init_callback(@_);
    $cb = $self->default_cb({json => 1}) if !$cb;

    http_request(
        DELETE => $self->_build_uri([$self->{path}, $bucket, $key]),
        headers => $self->_build_headers(),
        sub {
            $cv->send($cb->(@_));
        }
    );
    $cv;
}

1;
__END__

=head1 NAME

AnyEvent::Riak - Non-blocking Riak client

=head1 SYNOPSIS

    use AnyEvent::Riak;

    my $riak = AnyEvent::Riak->new(
      host => 'http://127.0.0.1:8098',
      path => 'riak',
    );

    die "Riak is not running" unless $riak->is_alive->recv;

    my $buckets = $riak->list_bucket('bucketname')->recv;
    my $new_bucket
        = $riak->set_bucket( 'foo', { props => { n_val => 2 } } )->recv;

For a complete description of the Riak REST API, please refer to
L<https://wiki.basho.com/display/RIAK/REST+API>.

=head1 DESCRIPTION

AnyEvent::Riak is a non-blocking riak client using C<AnyEvent>. This client allows you to connect to a Riak instance, create, modify and delete Riak objects.

=head2 METHODS

=over 4

=item B<is_alive>

Check if the Riak server is alive. If the ping is successful, 1 is returned,
else 0.

    # with callback
    my $ping = $riak->is_alive(sub {
        my $res = shift;
        if ($res) {
            # if everything is OK
        }else{
            # if something is wrong
        }
    });

    $ping->recv;

    #without callback
    my $ping = $riak->is_alive->recv;

=item B<list_bucket>

Get the schema and key list for 'bucket'. Possible options are:

=over 2

=item

props=[true|false] - whether to return the bucket properties

=item

keys=[true|false|stream] - whether to return the keys stored in the bucket

=back

If the operation failed, C<undef> is returned, else an hash reference
describing the bucket is returned.

    # with callback
    my $bucket = $riak->list_bucket('bucketname', {}, sub {
        my $struct = shift;
        if (scalar @{$struct->{keys}}) {
            # do something
        }
    });

    # without callback
    my $bucket = $riak->list_bucket(
        'bucketname',
        {
            keys  => 'true',
            props => 'false',
        }
    )->recv;

=item B<set_bucket>

Set the schema for 'bucket'. The schema parameter must be a hash with at least
an 'allowed_fields' field. Other valid fields are 'requried_fields',
'read_mask', and 'write_mask'.

    $riak->new_bucket('bucketname', {allowed_fields => '*'})->recv;

=item B<fetch>

Get the object stored in 'bucket' at 'key'.

    $riak->fetch('bucketname', 'key')->recv;

=item B<store>

Store 'object' in Riak. If the object has not defined its 'key' field, a key
will be chosen for it by the server.

    $riak->store({
        bucket => 'bucketname',
        key    => 'key',
        object => { foo => "bar", baz => 2 },
        links  => [],
    })->recv;

=item B<delete>

Delete the data stored in 'bucket' at 'key'.

    $riak->delete('bucketname', 'key')->recv;

=item B<walk>

Follow links from the object stored in 'bucket' at 'key' to other objects.
The 'spec' parameter should be an array of hashes, each hash optinally
defining 'bucket', 'tag', and 'acc' fields.  If a field is not defined in a
spec hash, the wildcard '_' will be used instead.

    ok $res = $jiak->walk(
        'bucketname',
        'key',
        [ { bucket => 'bucketname', key => '_' } ]
    )->recv;

=back

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2009 by linkfluence.

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
