package DANCER2;
use Dancer2;
use DBI;
use Data::Dumper;

our $VERSION = '0.1';

set list => 10;
set b_pageNum_list => 7;

get '/' => sub {
	template 'index' => { 'title' => 'DANCER2' };
};

get '/myboard/write' => sub {
	my $thread = params->{thread};
	my $depth = params->{depth};

	template 'write' => {'var1'=>$thread, 'var2'=>$depth }, { 'layout' => 'myboard' };
};

post '/myboard/edit' => sub {
	my $seq = params->{seq};
	my $pwd = params->{pwd};
	my $thread = params->{thread};
	my $depth = params->{depth};

	my $dbh = &connect_db;

	my $sql = <<EOF;
SELECT seq FROM ThreadBoard
WHERE seq = $seq AND pwd = '$pwd'
EOF
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $var1 = $sth->fetchrow_arrayref();

	if($var1->[0] eq "") {
	$sth->finish;
	$dbh->disconnect;

	return <<EOF;
<script> alert("암호가 일치하지 않습니다."); history.back(); </script>
EOF
	} else {

	$sql = <<EOF;
SELECT seq, thread, depth, writer, email, title, mode, ip, readcount, to_char(transdate, 'YY-MM-DD HH24:MI:SS') tdate, content
FROM ThreadBoard
WHERE seq = $seq
EOF

	$sth = $dbh->prepare($sql);
	$sth->execute();
	my $var1 = $sth->fetchrow_arrayref();
	#print Dumper($var1);

	$sth->finish;
	$dbh->disconnect;

	template 'edit' => { 'var1'=>$var1 }, { 'layout'=>'myboard' };

	}

};

post '/myboard/delete' => sub {
	my $seq = params->{seq};
	my $pwd = params->{pwd};

	my $dbh = &connect_db;

	my $sql = <<EOF;
SELECT seq FROM ThreadBoard
WHERE seq = $seq AND pwd = '$pwd'
EOF
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $var1 = $sth->fetchrow_arrayref();

	if($var1->[0] eq "") {
	$sth->finish;
	$dbh->disconnect;

	return <<EOF;
<script> alert("암호가 일치하지 않습니다."); history.back(); </script>
EOF
	} else {
	
	$sql = <<EOF;
DELETE from ThreadBoard
WHERE seq = $seq
EOF
	$sth = $dbh->prepare($sql);
	$sth->execute();

	$sth->finish;
	$dbh->disconnect;

	}
	
	#redirect '/myboard/list';
	return <<EOF;
<script> alert("글이 삭제되었습니다."); location.href='/myboard/list'; </script>
EOF

};


post '/myboard/insert' => sub {
	my $thread = params->{thread};
	my $depth = params->{depth};

	my $name = params->{name};
	my $mail = params->{mail};
	my $title = params->{title};
	my $memo = params->{memo};
	my $pwd = params->{pwd};
	my $ip = request->remote_address;
	
	my $dbh = &connect_db;

if($thread eq "") {
	my $sql = qq/
insert into threadboard(thread, depth, writer, pwd, email, title, mode, ip, content)
select coalesce(max(thread),0)+1000, 0, ?, ?, ?, ?, ?, ?, ?
from threadboard
	/;
#print "**** $name $mail $title **** $sql\n\n\n\n";
	my $sth = $dbh->prepare($sql);
	$sth->bind_param(1,$name);
	$sth->bind_param(2,$pwd);
	$sth->bind_param(3,$mail);
	$sth->bind_param(4,$title);
	$sth->bind_param(5,1);	
	$sth->bind_param(6,$ip);
	$sth->bind_param(7,$memo);
	
	$sth->execute();
	$sth->finish;
	$dbh->disconnect;
}
else {
	my $sql = <<EOF;
UPDATE ThreadBoard
SET thread = thread - 1
Where thread < $thread and thread > ($thread-1)/1000*1000
EOF
	my $sth = $dbh->prepare($sql);
	$sth->execute();

	$sql = <<EOF;
INSERT INTO ThreadBoard
(thread, depth, writer, pwd, email, title, mode, ip, content)
Values
($thread-1, $depth+1, '$name', '$pwd', '$mail', '$title', '1', '$ip', '$memo')
EOF
   $sth = $dbh->prepare($sql);
	$sth->execute();

	$sth->finish;
	$dbh->disconnect;
}

	return <<EOF;
<script> alert('저장되었습니다.'); location.href='/myboard/list'; </script>
EOF

};

post '/myboard/update' => sub {
	my $seq = params->{seq};

	my $mail = params->{mail};
	my $title = params->{title};
	my $memo = params->{memo};
	my $ip = request->remote_address;
	
	my $dbh = &connect_db;

	my $sql = qq/
UPDATE ThreadBoard
SET
	email = '$mail',
	title = '$title',
	mode = '1',
	ip = '$ip',
	content = '$memo'
WHERE
	seq = $seq
	/;
	
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	$sth->finish;
	$dbh->disconnect;

	return <<EOF;
<script> alert('수정되었습니다.'); location.href='/myboard/list'; </script>
EOF

};

get '/myboard/list' => sub {
#	my $list = 5;
#	my $pageNum = 1;
#	my $maxnum = $pageNum*$list;
#	my $minnum = ($pageNum-1)*$list+1;
	session('cnt' => 7);

	# pageing ===============
	my $list = setting('list');
	my $b_pageNum_list = setting('b_pageNum_list');

	my $pageNum = (params->{pageNum} ? params->{pageNum} : 1);
	my $block = int($pageNum/$b_pageNum_list+0.99);
	my $b_start_page = (($block-1)*$b_pageNum_list) + 1;
	my $b_end_page = $b_start_page + $b_pageNum_list - 1;
	# ========================



	my $dbh = &connect_db;

	my $sql = <<EOF;
SELECT count(*) cnt
FROM ThreadBoard
EOF
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $var1 = $sth->fetchrow_arrayref();

	# paging ===================
	my $total_count = $var1->[0];
	my $total_page = int($total_count/$list+0.99);
		if ($b_end_page > $total_page) {	$b_end_page = $total_page; }
	my $maxnum = $pageNum*$list; 
	my $minnum = ($pageNum-1)*$list;

	my $total_block = int($total_page/$b_pageNum_list+0.99);
	# ================================

	$sql = <<EOF;
SELECT * from (
SELECT a.*, ROW_NUMBER() OVER() AS rnum from (
select seq, thread, depth, writer, title, readcount, to_char(transdate, 'YY-MM-DD HH24:MI:SS') tdate, (EXTRACT (EPOCH FROM now() - transdate)/60/60)::int diffhour
from threadboard order by thread desc
fetch first $maxnum rows only
) a
) b WHERE rnum > $minnum
EOF

	$sth = $dbh->prepare($sql);
	$sth->execute();
	my $vars1 = $sth->fetchall_arrayref([]);
	# print Dumper($vars1);
	$sth->finish;
	$dbh->disconnect;
	

	template 'list' => { 'vars1'=>$vars1
	, 'pageNum'=>$pageNum 
	, 'block'=>$block
	, 'b_start_page'=>$b_start_page
	, 'b_end_page'=>$b_end_page
   , 'total_block'=>$total_block

	}, { 'layout'=>'myboard' };


};

get '/myboard/content' => sub {
	my $counter = session('cnt');

	my $board_idx = params->{'board_idx'};
	my $pageNum = params->{'pageNum'};

	my $dbh = &connect_db;

	if($counter == 7) {
	my $sql = <<EOF;
UPDATE ThreadBoard
SET readcount = readcount + 1
WHERE seq = $board_idx
EOF

	my $sth = $dbh->prepare($sql);
	$sth->execute();

	session('cnt' => 0);
	}

	my $sql = <<EOF;
SELECT seq, thread, depth, writer, email, title, mode, ip, readcount, to_char(transdate, 'YY-MM-DD HH24:MI:SS') tdate, content
FROM ThreadBoard
WHERE seq = $board_idx
EOF

	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $var1 = $sth->fetchrow_arrayref();
	#print Dumper($var1);

	$sth->finish;
	$dbh->disconnect;

	template 'content' => { 'var1'=>$var1 }, { 'layout'=>'myboard' };

};


sub connect_db {
   my $host = config->{host};
   my $port = config->{port};
   my $dbname = config->{dbname};
   my $username = config->{username};
   my $password = config->{password};

   my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port",
                               $username,
                               $password, 
                               { pg_utf8_flag => 1,
											pg_enable_utf8  => 1,
											AutoCommit => 1,
											RaiseError => 1,
											PrintError => 1,
										}
                            ) or die $DBI::errstr;

	return $dbh;
};


get '/test' => sub {
	#return '한글 깨짐';
	my $vars1 = {
	    article => 'The Third Shoe',
	    person  => { 
	        id    => 314, 
	        name  => 'Mr. Blue',
	        email => 'blue@nowhere.org',
	    },
	    primes  => [ 2, 3, 5, 7, 11, 13 ],
	    wizard  => sub { return join('---', 'Abracadabra!', @_) },
	    #cgi     => CGI->new('mode=submit&debug=1'),
	};
	my $var2 = "abcdefg";

	template 'test'=> { 'vars1'=>$vars1, 'var2'=>$var2 }, {'layout'=>'myboard'};

};

get '/pgtest' => sub {
	my $host = config->{host};
	my $port = config->{port};
	my $dbname = config->{dbname};
	my $username = config->{username};
	my $password = config->{password};
	
	my $dbh = DBI -> connect("dbi:Pg:dbname=$dbname;host=$host;port=$port",  
	                            $username,
	                            $password,
	                            {AutoCommit => 0, RaiseError => 1}
	                         ) or die $DBI::errstr;
	
	my $sth = $dbh->prepare("SELECT * FROM aaa");
	$sth->execute();                        # execute the query
	my @row;
	@row = $sth->fetchrow_array;

return <<EOF;
@row
<!-- <script> alert('sdfsd'); location.href='/myboard/list'; </script> -->
EOF

};

true;
