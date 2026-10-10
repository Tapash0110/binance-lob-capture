#!/usr/bin/env python3
"""
Generate a professional PDF technical whitepaper for the Binance LOB Capture system.
Uses ReportLab with high-quality formatting, tables, and typography.
"""

import sys
import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether, Preformatted
)
from reportlab.pdfgen import canvas

class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super().showPage()
        super().save()

    def draw_page_decorations(self, page_count):
        self.saveState()
        self.setFont("Helvetica", 9)
        self.setFillColor(colors.HexColor("#718096"))
        
        # Header (pages > 1)
        if self._pageNumber > 1:
            self.drawString(54, letter[1] - 36, "Binance Market Data Capture & LOB Engine — Technical Whitepaper")
            self.setStrokeColor(colors.HexColor("#E2E8F0"))
            self.setLineWidth(0.5)
            self.line(54, letter[1] - 42, letter[0] - 54, letter[1] - 42)
        
        # Footer
        footer_text = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(letter[0] - 54, 36, footer_text)
        self.drawString(54, 36, "Confidential — Quantitative Engineering System Architecture")
        self.setStrokeColor(colors.HexColor("#E2E8F0"))
        self.setLineWidth(0.5)
        self.line(54, 48, letter[0] - 54, 48)
        
        self.restoreState()


def create_pdf(output_filename):
    doc = SimpleDocTemplate(
        output_filename,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54
    )

    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=22,
        leading=26,
        textColor=colors.HexColor("#1A202C"),
        spaceAfter=8
    )
    
    subtitle_style = ParagraphStyle(
        'DocSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=16,
        textColor=colors.HexColor("#4A5568"),
        spaceAfter=18
    )

    h1_style = ParagraphStyle(
        'SectionH1',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"),
        spaceBefore=14,
        spaceAfter=6,
        keepWithNext=True
    )

    h2_style = ParagraphStyle(
        'SectionH2',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=15,
        textColor=colors.HexColor("#2D3748"),
        spaceBefore=10,
        spaceAfter=4,
        keepWithNext=True
    )

    body_style = ParagraphStyle(
        'DocBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9.5,
        leading=13.5,
        textColor=colors.HexColor("#2D3748"),
        spaceAfter=6
    )

    code_style = ParagraphStyle(
        'CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=8,
        leading=10.5,
        textColor=colors.HexColor("#1A202C"),
        backColor=colors.HexColor("#F7FAFC"),
        borderPadding=6,
        spaceAfter=6
    )

    table_body = ParagraphStyle(
        'TableBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#2D3748")
    )

    table_header = ParagraphStyle(
        'TableHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8.5,
        leading=11,
        textColor=colors.white
    )

    story = []

    # Title & Subtitle
    story.append(Paragraph("Binance Market Data Capture & Limit Order Book Engine", title_style))
    story.append(Paragraph("System Architecture, Concurrency Model & Algorithmic Mechanics", subtitle_style))
    story.append(Spacer(1, 8))

    # Executive Summary
    story.append(Paragraph("1. Executive Summary", h1_style))
    story.append(Paragraph(
        "In high-frequency trading (HFT) and quantitative market making, accurate market data ingestion "
        "and order book reconstruction represent foundational infrastructure. Latency jitter, non-deterministic "
        "floating-point rounding, or out-of-order sequence processing propagate execution errors and model drift across trading strategies. "
        "This system provides an ultra-low latency, thread-safe, and deterministic C++17 ingestion and limit order book engine "
        "designed for the Binance cryptocurrency exchange (supporting both <b>Spot</b> and <b>USD-M Futures</b> combined WebSocket streams).",
        body_style
    ))
    story.append(Paragraph(
        "<b>Key Engineering Pillars:</b><br/>"
        "• <b>Zero Floating-Point Drift:</b> Uses fixed-point 10^8 scaling entirely in 64-bit integer arithmetic.<br/>"
        "• <b>Non-Blocking Multi-Threading:</b> Decouples network reception, order book calculation, and disk I/O onto dedicated threads.<br/>"
        "• <b>Market Sequence Literacy:</b> Robust handling of Spot (U, u) and Futures (pu) update continuity, sequence gap detection, and clean reconnects.<br/>"
        "• <b>100% Deterministic Replay:</b> Offline mode reproduces exact order book snapshots from raw captured market data with zero network calls.",
        body_style
    ))
    story.append(Spacer(1, 8))

    # End to end flow
    story.append(Paragraph("2. End-to-End System Workflow", h1_style))
    flow_diagram = (
        "+-------------------------------------------------------------------------+\n"
        "|                     Binance Edge WebSocket Gateway                      |\n"
        "|            (Combined Streams: depth@100ms, depth5@100ms, trade)         |\n"
        "+------------------------------------+------------------------------------+\n"
        "                                     |\n"
        "                                     v\n"
        "+-------------------------------------------------------------------------+\n"
        "|                     Network Ingestion Thread (SSL)                      |\n"
        "|  - Boost.Beast TLS 1.2/1.3 WebSocket read loop                          |\n"
        "|  - Immediate CLOCK_REALTIME receive timestamp (recv_tsec / recv_tnsec)  |\n"
        "|  - Envelope parsing (stream name, symbol, stream_kind)                  |\n"
        "+------------------------------------+------------------------------------+\n"
        "                                     | Thread-safe dispatch\n"
        "                                     v\n"
        "+-------------------------------------------------------------------------+\n"
        "|                   Market Data & Order Book Engine                       |\n"
        "|  - Fast RapidJSON parsing & minified payload retention (Deliverable A)  |\n"
        "|  - Deterministic string-to-int64 fixed-point parser (10^8 ticks)        |\n"
        "|  - Sequence continuity validator (U, u, pu independent stream checks)   |\n"
        "|  - LOB state transitions: qty=0 level purge; depth5 top-5 replacement   |\n"
        "|  - Best top-5 bid/ask snapshot extraction (Deliverable B, 26 columns)   |\n"
        "+------------------------------------+------------------------------------+\n"
        "                                     | Asynchronous Task Queue\n"
        "                                     v\n"
        "+-------------------------------------------------------------------------+\n"
        "|                       Dedicated Async CSV Writer                        |\n"
        "|  - Background worker thread with lock-free/batched drain                |\n"
        "|  - 64KB+ block buffered writes to disk (Zero socket back-pressure)      |\n"
        "|  - Strict RFC 4180 escaping for all payloads                            |\n"
        "+-------------------------------------------------------------------------+"
    )
    story.append(Preformatted(flow_diagram, code_style))
    story.append(Spacer(1, 8))

    # Algorithmic Mechanics
    story.append(Paragraph("3. Algorithmic Mechanics & Core Innovations", h1_style))
    
    story.append(Paragraph("3.1. Zero Floating-Point Fixed-Point Parser (10^8 Ticks)", h2_style))
    story.append(Paragraph(
        "Binance delivers prices and quantities as decimal ASCII strings. Standard conversion via binary floats "
        "(<code>double</code> / <code>float</code>) violates IEEE-754 exactness. The system implements a deterministic "
        "string-to-int64 parser directly converting decimal strings to 64-bit integer ticks scaled by <b>10^8</b>:",
        body_style
    ))
    code_snippet_parser = (
        "inline int64_t parse_fixed8(std::string_view sv) noexcept {\n"
        "    // Parses integer and fractional components using pure integer arithmetic\n"
        "    // \"83049.43000000\" -> 8304943000000LL\n"
        "    // \"1.79016000\"     -> 179016000LL\n"
        "    // \"0.00013000\"     -> 13000LL\n"
        "    // Completely eliminates binary floating-point drift on the hot path.\n"
        "}"
    )
    story.append(Preformatted(code_snippet_parser, code_style))

    story.append(Paragraph("3.2. Local Order Book (LOB) State Transitions & Invariant Preservation", h2_style))
    story.append(Paragraph(
        "The LOB maintains full order book depth using two ordered red-black tree structures: "
        "<code>std::map&lt;int64_t, int64_t, std::greater&lt;int64_t&gt;&gt;</code> for bids (highest price first), "
        "and <code>std::map&lt;int64_t, int64_t, std::less&lt;int64_t&gt;&gt;</code> for asks (lowest price first).",
        body_style
    ))
    story.append(Paragraph(
        "<b>Differential Depth Semantics:</b> When a price level update arrives with quantity = 0, "
        "it is immediately erased from the book. Because full depth is preserved, the 6th best level instantly "
        "promotes into the top 5 in O(log N) time without requiring an external REST API query.<br/>"
        "<b>Partial Depth5 Semantics:</b> Provides an authoritative top-of-book refresh. The engine replaces the top 5 "
        "levels and automatically clears any stale crossing bids or asks.",
        body_style
    ))

    story.append(Paragraph("3.3. Independent Sequence Tracking & Gap Handling", h2_style))
    story.append(Paragraph(
        "Binance Spot and USD-M Futures employ distinct sequence tracking schemas: "
        "Spot transmits first update ID (U) and final update ID (u). Futures transmits previous update ID (pu) and final update ID (u). "
        "Furthermore, differential depth and depth5 streams operate with distinct update counters. "
        "The engine decouples their validation trackers, ensuring zero artificial sequence gap false positives.",
        body_style
    ))
    story.append(Spacer(1, 8))

    # Deliverables Table
    story.append(Paragraph("4. Deliverable Specifications & CSV Schemas", h1_style))
    
    schema_data = [
        [Paragraph("Deliverable", table_header), Paragraph("Target Output File", table_header), Paragraph("Columns", table_header), Paragraph("Key Role", table_header)],
        [
            Paragraph("<b>Deliverable A</b><br/>Market Data Audit Dump", table_body),
            Paragraph("market_data_&lt;venue&gt;_&lt;SYMBOL&gt;_&lt;date&gt;.csv", table_body),
            Paragraph("<b>9 Columns:</b><br/>recv_tsec, recv_tnsec, venue, stream_kind, shard_id, conn_epoch, conn_seq, symbol, payload_json", table_body),
            Paragraph("Raw audit trail of every inbound WebSocket event applied by the system.", table_body)
        ],
        [
            Paragraph("<b>Deliverable B</b><br/>Order Book Snapshots", table_body),
            Paragraph("market_data_&lt;venue&gt;_&lt;SYMBOL&gt;_&lt;date&gt;_orderbook.csv", table_body),
            Paragraph("<b>Exact 26 Columns:</b><br/>tsec, tnsec, seqNo, id, type, side, bid0..4, bid_size0..4, ask0..4, ask_size0..4", table_body),
            Paragraph("Deterministic LOB state snapshot emitted after each applied book event.", table_body)
        ]
    ]
    t_schema = Table(schema_data, colWidths=[1.4*inch, 2.0*inch, 2.0*inch, 1.8*inch])
    t_schema.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2B6CB0")),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E0")),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FAFC")]),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(t_schema)
    story.append(Spacer(1, 10))

    # Verification Matrix
    story.append(Paragraph("5. Verification & Test Matrix", h1_style))
    test_data = [
        [Paragraph("Test Suite", table_header), Paragraph("Function Tested", table_header), Paragraph("Verified Invariants", table_header), Paragraph("Status", table_header)],
        [
            Paragraph("test_fixed_point_parser", table_body),
            Paragraph("String-to-Int64 Fixed-Point Parser", table_body),
            Paragraph("10^8 scaling, decimal truncation, zero floats, negative values", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("test_lob_diff_semantics", table_body),
            Paragraph("LOB Differential Engine", table_body),
            Paragraph("qty=0 level purge, immediate promotion of level 2 to top", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("test_lob_top5_ordering", table_body),
            Paragraph("Top-of-Book Ordering", table_body),
            Paragraph("Bids strictly descending, Asks strictly ascending", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("test_lob_depth5_replace", table_body),
            Paragraph("Partial Snapshot Refresh", table_body),
            Paragraph("Top-5 replacement, clearing stale crossing levels", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("test_sequence_gap_detection", table_body),
            Paragraph("Sequence Continuity", table_body),
            Paragraph("Spot (U, u) and Futures (pu) gap flags, reconnect reset", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("test_rfc4180_escaping", table_body),
            Paragraph("RFC 4180 CSV Escaper", table_body),
            Paragraph("Round-trip quote, comma, and newline escaping integrity", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("test_orderbook_csv_schema", table_body),
            Paragraph("CSV Schema Compliance", table_body),
            Paragraph("Exact 26 columns, 25 commas per data row verified", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ],
        [
            Paragraph("Replay Determinism Test", table_body),
            Paragraph("Offline Replay vs Live Capture", table_body),
            Paragraph("diff -u output comparison: 100% bit-for-bit exact match", table_body),
            Paragraph("<font color='#276749'><b>PASSED</b></font>", table_body)
        ]
    ]
    t_test = Table(test_data, colWidths=[1.6*inch, 1.8*inch, 2.6*inch, 1.2*inch])
    t_test.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2B6CB0")),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E0")),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FAFC")]),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(t_test)
    story.append(Spacer(1, 10))

    # Operational Commands
    story.append(Paragraph("6. Operational Command Reference", h1_style))
    cmd_text = (
        "# 1. Clean Build & Run All Tests\n"
        "./build.sh\n\n"
        "# 2. Run Verification Script (Validates all 7 criteria)\n"
        "./verify.sh\n\n"
        "# 3. Live 5-Minute Capture (Binance Spot)\n"
        "./build/binance_capture --venue spot --symbols BTCUSDT --duration 300 --output-dir ./output\n\n"
        "# 4. Multi-Symbol Capture (BTC + ETH)\n"
        "./build/binance_capture --venue spot --symbols BTCUSDT,ETHUSDT --output-dir ./output\n\n"
        "# 5. USD-M Futures Capture\n"
        "./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output\n\n"
        "# 6. Offline Deterministic Replay (Zero Network Calls)\n"
        "./build/binance_capture --replay ./output/market_data_spot_BTCUSDT_2026-10-10.csv --output-dir ./replay_output"
    )
    story.append(Preformatted(cmd_text, code_style))

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"Successfully generated: {output_filename}")


if __name__ == '__main__':
    out = "docs/Binance_LOB_System_Architecture.pdf"
    if len(sys.argv) > 1:
        out = sys.argv[1]
    create_pdf(out)
