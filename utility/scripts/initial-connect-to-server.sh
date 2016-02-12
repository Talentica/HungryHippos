#!/usr/bin/expect

match_max 5000
set expect_out(buffer) {}

spawn ssh root@node_ip

expect {
"*(yes/no)?" {send "yes\r";exp_continue}
"'s password:" {send "node_initial_pwd\r";exp_continue}
"*current*" {send "node_initial_pwd\r";exp_continue}
"Enter*" {send "Jan2016!\r";exp_continue}
"Retype*" {send "Jan2016!\r";exp_continue}
"\\\$"  { puts "matched prompt"}
}
