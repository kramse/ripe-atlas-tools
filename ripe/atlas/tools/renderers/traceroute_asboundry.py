# -*- coding: utf-8 -*-

# Copyright (c) 2016 RIPE NCC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import socket
import sqlite3
import os
from math import ceil
from tzlocal import get_localzone
from sparklines import sparklines

from .base import Renderer as BaseRenderer

from ..helpers.colours import colourise
from ..helpers.sanitisers import sanitise
from ..ipdetails import IP


class Renderer(BaseRenderer):

    RENDERS = [BaseRenderer.TYPE_TRACEROUTE]
    TIME_FORMAT = "%a %b %d %H:%M:%S %Z %Y"
    DEFAULT_HIDE_ASNS = True
    DEFAULT_HIDE_ASN_ABNORMALITIES = True
    TIMING_BAR_WIDTH = 20
    DEFAULT_SHOW_HOSTNAMES = True
    DEFAULT_SQLITE = "~/rtts.sqlite"

    @staticmethod
    def add_arguments(parser):
        group = parser.add_argument_group(
            title="Optional arguments for traceroute_asboundry renderer"
        )
        group.add_argument(
            "--traceroute-show-hostnames",
            help="Show hostnames in the results",
            action="store_true",
            default=Renderer.DEFAULT_SHOW_HOSTNAMES
        )
        group.add_argument(
            "--traceroute-sqlite",
            help="sqlite file",
            default=Renderer.DEFAULT_SQLITE
        )

    def __init__(self, *args, **kwargs):
        BaseRenderer.__init__(self, *args, **kwargs)

        if "arguments" in kwargs:
            self.show_hostnames = kwargs["arguments"].traceroute_show_hostnames
            s = kwargs["arguments"].traceroute_sqlite
        else:
            self.show_hostnames = Renderer.DEFAULT_SHOW_HOSTNAMES
            s = Renderer.DEFAULT_SQLITE
        
        conn = sqlite3.connect(os.path.expanduser(s))
        self.c = conn.cursor()

    def _get_min_hop(self, packets):
        m = -1
        for p in packets:
            if p.rtt is None:
                continue
            if p.rtt < m or m is -1:
                m = p.rtt
        return m

    def _colourise_result(self, last, curr, next_, probe):
            
        last_min = self._get_min_hop(last.packets) 
        curr_min = self._get_min_hop(curr.packets)
        try:
            next_min = self._get_min_hop(next_.packets)
        except AttributeError:
            next_min = -1

        s = "select pct95,pct99 from rtts where prb_id is %s and ip is '%s' limit 1;" % (probe, curr.packets[0].origin) 
        
        pct95 = 0
        pct99 = 0

        self.c.execute(s)
        try:
            pct95, pct99 = self.c.fetchone()
        except TypeError:
            pass

        if pct99 > 0 and curr_min > pct99:
            return "red"

        if pct95 > 0 and curr_min > pct95:
            return "yellow"

        if next_min is -1 or last_min is -1 or curr_min is -1:
            return "white"

        if (last_min * 2) < curr_min:
            return "bold"

        return "white"

    def _find_max_latency(self, results):
        max_ = -1

        for r in results.hops:
            avg = self._get_avg_latency(r)
            if avg > max_:
                max_ = avg

        return max_

    def _get_avg_latency(self, hop):

        def avg(ttls):
            ttls_ = [e for e in ttls if e != None]
            if ttls_ == []:
                return -1
            return (sum(ttls_) / len(ttls_))

        return avg([p.rtt for p in hop.packets])

    def _format_latency_bar(self, avg, max_avg):
        width = self.TIMING_BAR_WIDTH / max_avg * int(avg)
        if width < 0:
            width = 0

        return "|" * int(width)

    def on_result(self, result):

        r = ""

        i = -1

        max_latency = self._find_max_latency(result)

        for hop in result.hops:
            i = i + 1

            if hop.is_error:
                r += "{}\n".format(
                    colourise(sanitise(hop.error_message), "red"))
                continue

            name = ""
            asn = ""
            name_padding = 28
            rtts = []
            for packet in hop.packets:
                if self.show_hostnames:
                    name_padding = 40
                    try:
                        h = socket.gethostbyaddr(packet.origin)[0]
                        name = (h[:42] + '..') if len(h) > 45 else h
                    except (socket.herror, TypeError):
                        name = '*'
                else:
                    name = name or packet.origin or "*"    
                if packet.origin and not asn:
                    asn = IP(packet.origin).asn
                if packet.rtt:
                    rtts.append("{:8} ms".format(packet.rtt))
                else:
                    rtts.append("          *")

            min_rtt = self._get_min_hop(hop.packets)
            try:
                next_ = result.hops[i+1]
            except IndexError:
                next_ = None
            colour = self._colourise_result(
                result.hops[i-1], hop, next_, result.probe_id
            )

            self._format_latency_bar(self._get_avg_latency(hop), max_latency)

            if not asn:
                tpl = "{hop:>3} {name:54} {rtts} {bars:8}\n"
            else:
                tpl = "{hop:>3} {name:45} {asn:>8} {rtts} {bars:8}\n"

            r += colourise(
                tpl.format(
                    hop=hop.index,
                    name=sanitise(name),
                    asn="AS{}".format(asn) if asn else "",
                    rtts="  ".join(rtts),
                    bars=self._format_latency_bar(
                        self._get_avg_latency(hop), max_latency)

                ),
                colour
            )
            

        created = result.created.astimezone(get_localzone())
        return "\n{}\n{}\n\n{}".format(
            colourise("Probe #{}".format(result.probe_id), "bold"),
            colourise(created.strftime(self.TIME_FORMAT), "bold"),
            r
        )
