package org.dist.consensus.zab

import java.net.InetAddress

import org.dist.kvstore.InetAddressAndPort
import org.scalatest.FunSuite

class LeaderElectionTest extends FunSuite {

  test("should set self vote to the vote with max id if zxids are same") {
    val server1 = InetAddressAndPort.create("10.10.10.10", 8000)
    val server2 = InetAddressAndPort.create("10.10.10.11", 8000)
    val server3 = InetAddressAndPort.create("10.10.10.12", 8000)
    val result = new Elector().elect(Map(server1 → Vote(1, 0), server2 → Vote(2, 0), server3 → Vote(3, 0)))

    assert(result.vote == Vote(3, 0))
  }

  test("server with max votes should win") {
    val server1 = InetAddressAndPort.create("10.10.10.10", 8000)
    val server2 = InetAddressAndPort.create("10.10.10.11", 8000)
    val server3 = InetAddressAndPort.create("10.10.10.12", 8000)
    val result = new Elector().elect(Map(server1 → Vote(3, 0), server2 → Vote(2, 0), server3 → Vote(3, 0)))

    assert(result.vote == Vote(3, 0))
    assert(result.winningCount == 2)
    assert(result.winningVote == Vote(3, 0))
  }
}
