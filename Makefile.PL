use ExtUtils::MakeMaker;

require 5.026;

# Use command 'oodist' to produce your whole software release.
my $version = '0.201';
my $webpages = "../public_html";
my $git      = "https://github.com/perl-Couch-DB";
my $publish  = "../public_html/couch-db";
my $homepage = "http://perl.overmeer.net/CPAN/";

my %oodist   = (
	oodoc_version => 3.06,
	first_year    => 2024,
	email         => 'markov@cpan.org',

	include  => [
	],

	use      => [
	],

	parser   => {
		syntax         => 'markov',
		skip_links     => [
		],
		pmhead         => undef,
	},

	tests    => {
	},

	release  => {
		publish        => "$publish/source",
	},

	raw      => {
		publish        => "$publish/raw",
	},

	generate => [ {
		# Add real pod to the releases
		format         => 'pod3',
		podtail        => undef,
	}, {
		export         => 'website',
		serializer     => 'json',
		markup         => 'html',
		publish        => "$publish/doctree",
		meta           => {
			main_manual    => 'Couch::DB',
		},
	}, ],
);

my %requires = (  # also included in t/00use.t
	'DateTime'			=> 0,
	'DateTime::Format::ISO8601'	=> 0,
	'DateTime::Format::Mail'	=> 0,
	'Devel::GlobalDestruction'  => 0,
	'HTTP::Status'		=> 6.39,
	'JSON'				=> 0,
	'List::Util'		=> 0,
	'Log::Report'		=> 0,
	'MIME::Base64'		=> 0,
# , 'Mojo::URL'			=> 0,  has no version, part of Mojolicious
# , 'Mojo::UserAgent'	=> 0,  has no version, part of Mojolicious
	'Mojolicious'		=> 0,
	'Scalar::Util'		=> 0,
	'Storable'			=> 0,
	'URI'				=> 0,
  );

WriteMakefile
	NAME       => 'Couch::DB',
	VERSION    => $version,
	AUTHOR     => 'Mark Overmeer',
	ABSTRACT   => 'thick CouchDB interface',
	PREREQ_PM  => \%prereq,
	LICENSE    => 'perl_5',

	META_MERGE => {
		'meta-spec' => { version => 2 },
		resources   => {
			repository => {
				type => 'git',
				url  => "$git.git",
				web  => $git,
			},
			bugtracker => {
				web  => "$git/issues",
			},
			homepage => $homepage,
			license  => [ 'http//dev.perl.org/licenses/' ],
		},
		prereqs => {
			runtime => {
				requires   => \%requires,
				recommends => {},
				suggests   => {},
			},
			develop => {
				requires => {
					'OODoc' => '3.00',
				},
			},
			test => {
				requires => {
					'Test::More' => 1.00,
					'Test::Pod'  => 1.00,
				},
			},
		},

		# You may use multiple set-ups, see "oodist --make"
		x_oodist => \%oodist,
	};

sub MY::postamble { <<'__POSTAMBLE' }

reference::
	bin/reference-table >reference.html
	cp reference.html ../public_html/couch-db/reference.html

__POSTAMBLE
