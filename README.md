

1	20,00	20,00
2	19,00	21,40
3	18,05	22,90
4	17,15	24,50
5	16,29	26,22
6	15,48	28,05
7	14,70	30,01
8	13,97	32,12
9	13,27	34,36
10	12,60	36,77
11	11,97	39,34
12	11,38	42,10
13	10,81	45,04
14	10,27	48,20
15	9,75	51,57
16	9,27	55,18
17	8,80	59,04
18	8,36	63,18
19	7,94	67,60
20	7,55	72,33
21	7,17	77,39
22	6,81	82,81
23	6,47	88,61
24	6,15	94,81
25	5,84	101,45
26	5,55	108,55
27	5,27	116,15
28	5,01	124,28
29	4,76	132,98
30	4,52	142,29
31	4,29	152,25
32	4,08	162,90
33	3,87	174,31
34	3,68	186,51
35	3,50	199,56
36	3,32	213,53
37	3,16	228,48
38	3,00	244,47
39	2,85	261,59
40	2,71	279,90
41	2,57	299,49
42	2,44	320,45
43	2,32	342,89
44	2,20	366,89
45	2,09	392,57
46	1,99	420,05
47	1,89	449,45
48	1,79	480,91
49	1,71	514,58
50	1,62	550,60
51	1,54	589,14
52	1,46	630,38
53	1,39	674,51
54	1,32	721,72
55	1,25	772,24
56	1,19	826,30
57	1,13	884,14
58	1,07	946,03
59	1,02	1012,25
60	0,97	1083,11
61	0,92	1158,93
62	0,88	1240,05
63	0,83	1326,86
64	0,79	1419,74
65	0,75	1519,12
66	0,71	1625,46
67	0,68	1739,24
68	0,64	1860,99
69	0,61	1991,25
70	0,58	2130,64
71	0,55	2279,79
72	0,52	2439,37
73	0,50	2610,13
74	0,47	2792,84
75	0,45	2988,34
76	0,43	3197,52
77	0,41	3421,35
78	0,39	3660,84
79	0,37	3917,10
80	0,35	4191,30
81	0,33	4484,69
82	0,31	4798,62
83	0,30	5134,52
84	0,28	5493,94
85	0,27	5878,51
86	0,26	6290,01
87	0,24	6730,31
88	0,23	7201,43
89	0,22	7705,53
90	0,21	8244,92
91	0,20	8822,06
92	0,19	9439,60
93	0,18	10100,38
94	0,17	10807,40
95	0,16	11563,92
96	0,15	12373,39
97	0,15	13239,53
98	0,14	14166,30
99	0,13	15157,94
100	0,12	16219,00
101	0,12	17354,33
102	0,11	18569,13
103	0,11	19868,97
104	0,10	21259,80
105	0,10	22747,98
106	0,09	24340,34
107	0,09	26044,16


```mermaid
graph TD
    %% ==================== MAIN ENTRY ====================
    Start([main start]) --> Init[Инициализация: env, pool, exchange]
    Init --> ClearBots[clear_orders_ids_for_bots]
    ClearBots --> BatchCancel[batch_cancel_stop_orders]
    BatchCancel --> CleanLoop[loop: auto_clean_account]
    CleanLoop --> WSMainLoop[main WebSocket loop]

    %% ==================== WEBSOCKET CONNECTION ====================
    subgraph "WebSocket Connection Layer"
        WSMainLoop --> GetWSUrl[get_private_ws_url]
        GetWSUrl --> KuCoinClientAPI1[KuCoinClient::api_v1_bullet_private]
        GetWSUrl --> ConnectWS[connect_async]
        ConnectWS --> Subscribe[build_subscription]
        Subscribe --> SendSubscribe[event_ws_write.send]
        SendSubscribe --> WSLoop[tokio::select! loop]
        
        WSLoop --> ReadMsg[event_ws_read.next]
        ReadMsg --> |Text| SendToChannel[tx_in.send]
        WSLoop --> PingTick[event_ping_interval.tick]
        PingTick --> SendPing[event_ws_write.send Ping]
    end

    %% ==================== MESSAGE PROCESSING ====================
    subgraph "Message Processing Pipeline"
        SendToChannel --> SpawnProcess[spawn_process_kcn_msg]
        SpawnProcess --> ProcessMsg[process_kcn_msg]
        ProcessMsg --> DeserializeMsg[serde_json::from_str KuCoinMessage]
        
        DeserializeMsg --> |Welcome/Ack/Error| LogEvent[insert_db_event/error]
        DeserializeMsg --> |Message| RouteTopic{topic}
        
        RouteTopic --> |/account/balance| BalanceFlow
        RouteTopic --> |/spotMarket/tradeOrdersV2| OrderFlow
        RouteTopic --> |/spotMarket/advancedOrders| AdvancedFlow
        RouteTopic --> |/margin/position| PositionFlow
    end

    %% ==================== BALANCE FLOW ====================
    subgraph "Balance Flow"
        BalanceFlow --> ParseBalance[BalanceData::deserialize]
        ParseBalance --> InsertBalance[insert_db_balance]
    end

    %% ==================== ORDER FLOW ====================
    subgraph "Order Flow"
        OrderFlow --> ParseOrder[OrderData::deserialize]
        ParseOrder --> HandleOrder[handle_trade_order_event]
        HandleOrder --> InsertOrderEvent[insert_db_orderevent]
        
        HandleOrder --> CheckBotType{check client_oid in bots}
        
        CheckBotType --> |exit_tp_client_oid| TPComplete[TP Order Complete]
        TPComplete --> DeleteTP[delete_exit_tp_id_bot_by_client_oid]
        TPComplete --> CancelSLOrder[api_v3_hf_margin_stop_order_cancel_by_client_oid]
        TPComplete --> DeleteSL[delete_exit_sl_id_bot_by_client_oid]
        TPComplete --> GetMatchValueTP[get_total_match_value_by_client_oid]
        TPComplete --> UpdateBalanceTP[update_balance_bot_by_exit_tp_client_oid]
        TPComplete --> NewRandomTrade1[make_random_trade]
        
        CheckBotType --> |exit_sl_client_oid| SLComplete[SL Order Complete]
        SLComplete --> DeleteSL2[delete_exit_sl_id_bot_by_client_oid]
        SLComplete --> CancelTPOrder[api_v3_hf_margin_stop_order_cancel_by_client_oid]
        SLComplete --> DeleteTP2[delete_exit_tp_id_bot_by_client_oid]
        SLComplete --> GetMatchValueSL[get_total_match_value_by_client_oid]
        SLComplete --> UpdateBalanceSL[update_balance_bot_by_exit_sl_client_oid]
        SLComplete --> NewRandomTrade2[make_random_trade]
        
        CheckBotType --> |entry_client_oid| EntryComplete[Entry Order Complete]
        EntryComplete --> UpdateBalanceEntry[update_bot_balance_by_entry_client_oid]
        EntryComplete --> CalcTPSL{calculate TP/SL based on side}
        
        CalcTPSL --> |side=buy| BuyStopOrders
        CalcTPSL --> |side=sell| SellStopOrders
        
        BuyStopOrders --> UpdateTPClientBuy[update_exit_tp_client_oid_bot_by_entry_client_oid]
        BuyStopOrders --> UpdateSLClientBuy[update_exit_sl_client_oid_bot_by_entry_client_oid]
        BuyStopOrders --> CreateTPBuy[api_v3_hf_margin_stop_order TP +7%]
        BuyStopOrders --> CreateSLBuy[api_v3_hf_margin_stop_order SL -5%]
        BuyStopOrders --> LinkOrderIdsBuy[update_exit_tp/sl_order_id]
        
        SellStopOrders --> UpdateTPClientSell[update_exit_tp_client_oid_bot_by_entry_client_oid]
        SellStopOrders --> UpdateSLClientSell[update_exit_sl_client_oid_bot_by_entry_client_oid]
        SellStopOrders --> CreateTPSell[api_v3_hf_margin_stop_order TP -7%]
        SellStopOrders --> CreateSLSell[api_v3_hf_margin_stop_order SL +5%]
        SellStopOrders --> LinkOrderIdsSell[update_exit_tp/sl_order_id]
        
        EntryComplete --> ClearEntry[set_null_entry_client_oid_by_entry_client_oid]
    end

    %% ==================== ADVANCED ORDERS FLOW (Error Recovery) ====================
    subgraph "Advanced Orders Flow"
        AdvancedFlow --> ParseAdvanced[AdvancedOrders::deserialize]
        ParseAdvanced --> CheckError{error exists?}
        CheckError --> |Yes| RetryLoop[Retry Loop MAX 1000]
        RetryLoop --> UpdateBotRef[update_exit_sl/tp_client_oid_bot_by_order_id]
        UpdateBotRef --> DetermineType{stop type}
        DetermineType --> |loss| HandleLoss
        DetermineType --> |entry| HandleEntry
        
        HandleLoss --> |side=buy| RecreateFundsOrder[make_hf_funds_margin_order]
        HandleLoss --> |side=sell| RecreateSizeOrder[make_hf_size_margin_order]
        HandleEntry --> |side=buy| RecreateFundsOrder
        HandleEntry --> |side=sell| RecreateSizeOrder
    end

    %% ==================== POSITION FLOW ====================
    subgraph "Position Flow"
        PositionFlow --> ParsePosition[PositionData::deserialize]
        ParsePosition --> RepayLoans[Repay borrows from asset_list]
        RepayLoans --> CreateRepay[create_repay_order]
        ParsePosition --> UpsertRatio[upsert_position_ratio]
        ParsePosition --> UpsertDebt[upsert_position_debt]
        ParsePosition --> UpsertAsset[upsert_position_asset]
    end

    %% ==================== AUTO CLEAN ACCOUNT ====================
    subgraph "Auto Clean Account"
        CleanLoop --> GetMarginAcc[get_all_margin_accounts]
        GetMarginAcc --> ForEachCurrency[for each currency in accounts]
        
        ForEachCurrency --> CheckLiability{liability > 0?}
        CheckLiability --> |yes| CheckAvailable{available >= liability?}
        CheckAvailable --> |yes| FullRepay[create_repay_order full]
        CheckAvailable --> |partial| PartialRepay[create_repay_order partial]
        CheckAvailable --> |no & not USDT| BuyToRepay[Buy tokens with margin]
        
        BuyToRepay --> GetPriceBuy[get_ticker_price]
        BuyToRepay --> GetSymbolBuy[fetch_symbol_info_by_symbol]
        BuyToRepay --> MakeMarginBuy[make_hf_funds_margin_order]
        
        CheckLiability --> |no & not USDT| CheckExcess{available > 0?}
        CheckExcess --> |yes| CheckMinAmount{below min?}
        CheckMinAmount --> |yes| TransferToTrade[sent_account_transfer]
        CheckMinAmount --> |no| SellExcessTokens[make_hf_size_margin_order]
    end

    %% ==================== INITIAL ORDERS ====================
    subgraph "Initial Orders Creation"
        SpawnProcess --> InitOrdersCheck{init_order_execute?}
        InitOrdersCheck --> |first time| CreateInitOrders[create_init_orders]
        CreateInitOrders --> GetAllBots[get_all_bots_for_trade]
        GetAllBots --> ForEachBot[for each bot]
        ForEachBot --> MakeRandomTrade
    end

    %% ==================== RANDOM TRADE LOGIC ====================
    subgraph "Random Trade Logic"
        MakeRandomTrade --> RetryRandom[Retry Loop MAX 10]
        RetryRandom --> GetRandomSymbol[get_random_symbol]
        GetRandomSymbol --> GetSymbolInfo[fetch_symbol_info_by_symbol]
        GetSymbolInfo --> UpdateBotEntry[update_bot_entry_client_oid_by_id]
        UpdateBotEntry --> GetRandomSide[get_random_side]
        
        GetRandomSide --> |sell| SellFlow
        GetRandomSide --> |buy| BuyFlow
        
        SellFlow --> GetPriceSell[get_ticker_price]
        GetPriceSell --> CalcSize[token_size = balance / price]
        CalcSize --> MakeSizeOrder[make_hf_size_margin_order]
        
        BuyFlow --> MakeFundsOrder[make_hf_funds_margin_order]
        
        MakeSizeOrder --> OnError1[update_bot_entry_client_oid_by_id NULL]
        MakeFundsOrder --> OnError2[update_bot_entry_client_oid_by_id NULL]
    end

    %% ==================== HTTP REQUESTS LAYER ====================
    subgraph "KuCoin HTTP Client"
        GetWSUrl --> KuCoinClient1[KuCoinClient]
        BatchCancel --> KuCoinClient2
        GetMarginAcc --> KuCoinClient3
        CreateRepay --> KuCoinClient4
        CancelSLOrder --> KuCoinClient5
        CancelTPOrder --> KuCoinClient6
        CreateTPBuy --> KuCoinClient7
        CreateSLBuy --> KuCoinClient8
        CreateTPSell --> KuCoinClient9
        CreateSLSell --> KuCoinClient10
        GetPriceBuy --> KuCoinClient11
        GetPriceSell --> KuCoinClient12
        
        KuCoinClient1 --> MakeRequest[make_request]
        KuCoinClient2 --> MakeRequest
        KuCoinClient3 --> MakeRequest
        KuCoinClient4 --> MakeRequest
        KuCoinClient5 --> MakeRequest
        KuCoinClient6 --> MakeRequest
        KuCoinClient7 --> MakeRequest
        KuCoinClient8 --> MakeRequest
        KuCoinClient9 --> MakeRequest
        KuCoinClient10 --> MakeRequest
        KuCoinClient11 --> MakeRequest
        KuCoinClient12 --> MakeRequest
        
        MakeRequest --> GenSignature[generate_signature HMAC-SHA256]
        MakeRequest --> GenPassphrase[generate_passphrase_signature]
        MakeRequest --> AddHeaders[KC-API-* headers]
        MakeRequest --> SendHTTP[reqwest::Client.send]
    end

    %% ==================== DATABASE LAYER ====================
    subgraph "Database Operations"
        InsertBalance --> DB[(PostgreSQL)]
        InsertOrderEvent --> DB
        LogEvent --> DB
        InsertError[insert_db_error] --> DB
        GetMatchValueTP --> DB
        UpdateBalanceTP --> DB
        UpdateBalanceSL --> DB
        UpdateBalanceEntry --> DB
        GetRandomSymbol --> DB
        GetSymbolInfo --> DB
        UpdateBotEntry --> DB
        UpdateTPClientBuy --> DB
        UpdateSLClientBuy --> DB
        UpdateTPClientSell --> DB
        UpdateSLClientSell --> DB
        LinkOrderIdsBuy --> DB
        LinkOrderIdsSell --> DB
        DeleteTP --> DB
        DeleteSL --> DB
        ClearEntry --> DB
        UpsertRatio --> DB
        UpsertDebt --> DB
        UpsertAsset --> DB
        GetAllBots --> DB
        UpdateBotRef --> DB
    end

    %% ==================== UTILITIES ====================
    subgraph "Utilities"
        GetRandomSide --> FastRand[fastrand::bool]
        BuildSubscription --> SubJSON[subscription JSONs]
        FormatAssert --> CalcPrecision[calculate precision]
        FormatAssert --> RoundSize[round by increment]
    end

    %% ==================== STYLES ====================
    style Start fill:#90EE90
    style SpawnProcess fill:#87CEEB
    style HandleOrder fill:#FFD700
    style AutoClean fill:#FFB6C6
    style MakeRandomTrade fill:#98FB98
    style MakeRequest fill:#FFA500
    style DB fill:#4169E1,color:#white
    style WSMainLoop fill:#9370DB
    style RetryLoop fill:#FF6347
```