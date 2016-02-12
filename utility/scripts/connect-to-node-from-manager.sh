#!/usr/bin/expect

match_max 5000
set expect_out(buffer) {}

spawn ssh root@node_ip

expect {
"*(yes/no)?" {send "yes\r";exp_continue}
"'s password:" {send "node_initial_pwd\r";exp_continue}
"\\\$"  { puts "matched prompt"}
}
