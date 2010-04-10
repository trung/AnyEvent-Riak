package AnyEvent::Riak;

use strict;
use Carp;
use URI;
use JSON::XS;
use AnyEvent;
use AnyEvent::HTTP;
use Data::UUID;

our $VERSION = '0.02';

sub new {
    my ( $class, %args ) = @_;

    my $uuid = Data::UUID->new;

    my $host = delete $args{host} || 'http://127.0.0.1:8098';
    my $path = delete $args{path} || 'riak';
    my $clientId = delete $args{clientId} || $uuid->create_str;

    bless {
        host => $host,
        path => $path,
	clientId => $clientId,
        %args,
    }, $class;
}

sub set_bucket {
    my ( $self, $bucket, $schema ) = @_;

    $self->_request(
        'PUT', $self->_build_uri( [$bucket] ),
        '204', encode_json { props => $schema }
    );
}

sub list_bucket {
    my ( $self, $bucket ) = @_;
    return $self->_request( 'GET', $self->_build_uri( [$bucket] ), '200' );
}

sub fetch {
    my ( $self, $bucket, $key, $r ) = @_;
    $r = $self->{r} || 2 if !$r;
    return $self->_request( 'GET',
        $self->_build_uri( [ $bucket, $key ], { r => $r } ), '200,300,304' );
}

sub store {
    my ( $self, $object, $w, $dw, ) = @_;

    $w  = $self->{w}  || 2 if !$w;
    $dw = $self->{dw} || 2 if !$dw;

    my $bucket = $object->{bucket};
    my $key    = $object->{key};
    $object->{links} = [] if !exists $object->{links};

	# Normal status codes: 200 OK, 204 No Content, 300 Multiple Choices. 
	# FIXME Links must be set in the Links header
    return $self->_request(
        'PUT',
        $self->_build_uri(
            [ $bucket, $key ],
            {
                w          => $w,
                dw         => $dw,
                returnbody => 'true'
            }
        ),
        '200,204,300',
        encode_json $object);
}

sub delete {
    my ( $self, $bucket, $key, $rw ) = @_;

    $rw = $self->{rw} || 2 if !$rw;
    return $self->_request( 'DELETE',
        $self->_build_uri( [ $bucket, $key ], { dw => $rw } ), 204 );
}

# FIXME doesn't work. Must handle multipart/fixed returned content
sub walk {
    my ( $self, $bucket, $key, $spec ) = @_;
    my $path = $self->_build_uri( [ $bucket, $key ] );
    $path .= $self->_build_spec($spec);
    return $self->_request( 'GET', $path, 200 );
}

sub get_clientId {
    my ($self) = @_;
    return $self->{clientId};
}

sub _build_spec {
    my ( $self, $spec ) = @_;
    my $acc = '/';
    foreach my $item (@$spec) {
        $acc
            .= ( $item->{bucket} || '_' ) . ','
            . ( $item->{tag} || '_' ) . ','
            . ( $item->{acc} || '_' ) . '/';
    }
    return $acc;
}

sub _build_uri {
    my ( $self, $path, $query ) = @_;
    my $uri = URI->new( $self->{host} );
    $uri->path( $self->{path} . "/" . join( "/", @$path ) );
    $uri->query_form(%$query) if $query;
    return $uri->as_string;
}

sub _request {
    my ( $self, $method, $uri, $expected, $body ) = @_;
    my $cv = AnyEvent->condvar;

    my $cb = sub {
        my ( $body, $headers ) = @_;
        if ( $expected =~ m/$headers->{Status}/ ) {
			eval {
				if ($body) {
					return $cv->send( decode_json($body) );
				} else {
					return $cv->send(1);
				}
			};
			if ($@) {
				return $cv->croak(
	                JSON::XS->new->pretty(1)->encode( { method => $method, 
								uri => $uri, 
								body => $body,
								status => $headers->{Status},
								reason => $headers->{Reason},
								error => $@ } ) );
			}
        }
        else {
            return $cv->croak(
                JSON::XS->new->pretty(1)->encode( { method => $method, 
							uri => $uri, 
							body => $body,
							status => $headers->{Status},
							reason => $headers->{Reason}}) );
        }
    };
    if ($body) {
        http_request(
            $method => $uri,
            headers => $self->_build_headers,
            body    => $body,
            $cb
        );
    }
    else {
        http_request(
            $method => $uri,
            headers => $self->_build_headers,
            $cb
        );
    }
    $cv;
}

sub _build_headers {
    my ($self) = @_;
    return { 
	'Content-Type' => 'application/json', 
	'X-Riak-ClientId' => $self->{clientId},
    };
}

1;
__END__

=head1 NAME

AnyEvent::Riak - Non-blocking Riak client

=head1 SYNOPSIS

  use AnyEvent::Riak;

  my $riak = AnyEvent::Riak->new(
    host => 'http://127.0.0.1:8098',
    path => 'jiak',
  );

  my $buckets    = $riak->list_bucket('bucketname')->recv;
  my $new_bucket = $riak->set_bucket('foo', {allowed_fields => '*'})->recv;
  my $store      = $riak->store({bucket => 'foo', key => 'bar', object => {baz => 1},link => []})->recv;
  my $fetch      = $riak->fetch('foo', 'bar')->recv;
  my $delete     = $riak->delete('foo', 'bar')->recv;

=head1 DESCRIPTION

AnyEvent::Riak is a non-blocking riak client using anyevent.

=head2 METHODS

=over 4

=item B<list_bucket>

Get the schema and key list for 'bucket'

    $riak->list_bucket('bucketname')->recv;

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
