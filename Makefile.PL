use inc::Module::Install;
name 'AnyEvent-Riak';
all_from 'lib/AnyEvent/Riak.pm';

requires 'URI';
requires 'JSON';
requires 'AnyEvent';
requires 'AnyEvent::HTTP';
requires 'Data::UUID';

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::More';
build_requires 'Test::Exception';
use_test_base;
auto_include;
# auto_set_repository;
WriteAll;
