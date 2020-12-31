from odbc_conn import connector
import pandas as pd
from google.oauth2 import service_account

# credentials = service_account.Credentials.from_service_account_file("ga_auth.json")
credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\jphan\Repositories\Tuesday Meeting\tuesday_meeting\ga_auth.json")
conn = connector()


def pull_user_list_sql(year_filter=2018, month_filter=7):
    """This function queries the Clark db tblOrders table for prior customers and performs a number of data operations:
        - creates a lookup table of iso format country codes
        - parses first and last name from the name field
        - cleans up phone number to remove inappropriate symbols and formats
        - adds RFM score
        - de-dups results
    The final table contains ('user_email', 'firstname', 'lastname', 'country_iso', 'zip', 'international_phone',
    'rfm' and 'rfm_category').
    Table can be filtered for dates greater than or equal to year and month.
    """

    # create sql query
    customer_list_sql = '''
    SET NOCOUNT ON;
    IF OBJECT_ID('tempdb..#country_list') IS NOT NULL
    BEGIN
        DROP TABLE #country_list;
    END

    IF OBJECT_ID('tempdb..#country_format') IS NOT NULL
    BEGIN
        DROP TABLE #country_format;
    END

    IF OBJECT_ID('tempdb..#all_orders') IS NOT NULL
    BEGIN
        DROP TABLE #all_orders;
    END

    CREATE TABLE #country_list (
      id int,
      iso varchar(2),
      name varchar(80) ,
      nicename varchar(80) ,
      iso3 char(3) ,
      numcode int ,
      phonecode int 
    );

    INSERT INTO #country_list VALUES
    (1, 'AF', 'AFGHANISTAN', 'Afghanistan', 'AFG', 4, 93),
    (2, 'AL', 'ALBANIA', 'Albania', 'ALB', 8, 355),
    (3, 'DZ', 'ALGERIA', 'Algeria', 'DZA', 12, 213),
    (4, 'AS', 'AMERICAN SAMOA', 'American Samoa', 'ASM', 16, 1684),
    (5, 'AD', 'ANDORRA', 'Andorra', 'AND', 20, 376),
    (6, 'AO', 'ANGOLA', 'Angola', 'AGO', 24, 244),
    (7, 'AI', 'ANGUILLA', 'Anguilla', 'AIA', 660, 1264),
    (8, 'AQ', 'ANTARCTICA', 'Antarctica', NULL, NULL, 0),
    (9, 'AG', 'ANTIGUA AND BARBUDA', 'Antigua and Barbuda', 'ATG', 28, 1268),
    (10, 'AR', 'ARGENTINA', 'Argentina', 'ARG', 32, 54),
    (11, 'AM', 'ARMENIA', 'Armenia', 'ARM', 51, 374),
    (12, 'AW', 'ARUBA', 'Aruba', 'ABW', 533, 297),
    (13, 'AU', 'AUSTRALIA', 'Australia', 'AUS', 36, 61),
    (14, 'AT', 'AUSTRIA', 'Austria', 'AUT', 40, 43),
    (15, 'AZ', 'AZERBAIJAN', 'Azerbaijan', 'AZE', 31, 994),
    (16, 'BS', 'BAHAMAS', 'Bahamas', 'BHS', 44, 1242),
    (17, 'BH', 'BAHRAIN', 'Bahrain', 'BHR', 48, 973),
    (18, 'BD', 'BANGLADESH', 'Bangladesh', 'BGD', 50, 880),
    (19, 'BB', 'BARBADOS', 'Barbados', 'BRB', 52, 1246),
    (20, 'BY', 'BELARUS', 'Belarus', 'BLR', 112, 375),
    (21, 'BE', 'BELGIUM', 'Belgium', 'BEL', 56, 32),
    (22, 'BZ', 'BELIZE', 'Belize', 'BLZ', 84, 501),
    (23, 'BJ', 'BENIN', 'Benin', 'BEN', 204, 229),
    (24, 'BM', 'BERMUDA', 'Bermuda', 'BMU', 60, 1441),
    (25, 'BT', 'BHUTAN', 'Bhutan', 'BTN', 64, 975),
    (26, 'BO', 'BOLIVIA', 'Bolivia', 'BOL', 68, 591),
    (27, 'BA', 'BOSNIA AND HERZEGOVINA', 'Bosnia and Herzegovina', 'BIH', 70, 387),
    (28, 'BW', 'BOTSWANA', 'Botswana', 'BWA', 72, 267),
    (29, 'BV', 'BOUVET ISLAND', 'Bouvet Island', NULL, NULL, 0),
    (30, 'BR', 'BRAZIL', 'Brazil', 'BRA', 76, 55),
    (31, 'IO', 'BRITISH INDIAN OCEAN TERRITORY', 'British Indian Ocean Territory', NULL, NULL, 246),
    (32, 'BN', 'BRUNEI DARUSSALAM', 'Brunei Darussalam', 'BRN', 96, 673),
    (33, 'BG', 'BULGARIA', 'Bulgaria', 'BGR', 100, 359),
    (34, 'BF', 'BURKINA FASO', 'Burkina Faso', 'BFA', 854, 226),
    (35, 'BI', 'BURUNDI', 'Burundi', 'BDI', 108, 257),
    (36, 'KH', 'CAMBODIA', 'Cambodia', 'KHM', 116, 855),
    (37, 'CM', 'CAMEROON', 'Cameroon', 'CMR', 120, 237),
    (38, 'CA', 'CANADA', 'Canada', 'CAN', 124, 1),
    (39, 'CV', 'CAPE VERDE', 'Cape Verde', 'CPV', 132, 238),
    (40, 'KY', 'CAYMAN ISLANDS', 'Cayman Islands', 'CYM', 136, 1345),
    (41, 'CF', 'CENTRAL AFRICAN REPUBLIC', 'Central African Republic', 'CAF', 140, 236),
    (42, 'TD', 'CHAD', 'Chad', 'TCD', 148, 235),
    (43, 'CL', 'CHILE', 'Chile', 'CHL', 152, 56),
    (44, 'CN', 'CHINA', 'China', 'CHN', 156, 86),
    (45, 'CX', 'CHRISTMAS ISLAND', 'Christmas Island', NULL, NULL, 61),
    (46, 'CC', 'COCOS (KEELING) ISLANDS', 'Cocos (Keeling) Islands', NULL, NULL, 672),
    (47, 'CO', 'COLOMBIA', 'Colombia', 'COL', 170, 57),
    (48, 'KM', 'COMOROS', 'Comoros', 'COM', 174, 269),
    (49, 'CG', 'CONGO', 'Congo', 'COG', 178, 242),
    (50, 'CD', 'CONGO, THE DEMOCRATIC REPUBLIC OF THE', 'Congo, the Democratic Republic of the', 'COD', 180, 242),
    (51, 'CK', 'COOK ISLANDS', 'Cook Islands', 'COK', 184, 682),
    (52, 'CR', 'COSTA RICA', 'Costa Rica', 'CRI', 188, 506),
    (53, 'CI', 'COTE D''IVOIRE', 'Cote D''Ivoire', 'CIV', 384, 225),
    (54, 'HR', 'CROATIA', 'Croatia', 'HRV', 191, 385),
    (55, 'CU', 'CUBA', 'Cuba', 'CUB', 192, 53),
    (56, 'CY', 'CYPRUS', 'Cyprus', 'CYP', 196, 357),
    (57, 'CZ', 'CZECH REPUBLIC', 'Czech Republic', 'CZE', 203, 420),
    (58, 'DK', 'DENMARK', 'Denmark', 'DNK', 208, 45),
    (59, 'DJ', 'DJIBOUTI', 'Djibouti', 'DJI', 262, 253),
    (60, 'DM', 'DOMINICA', 'Dominica', 'DMA', 212, 1767),
    (61, 'DO', 'DOMINICAN REPUBLIC', 'Dominican Republic', 'DOM', 214, 1809),
    (62, 'EC', 'ECUADOR', 'Ecuador', 'ECU', 218, 593),
    (63, 'EG', 'EGYPT', 'Egypt', 'EGY', 818, 20),
    (64, 'SV', 'EL SALVADOR', 'El Salvador', 'SLV', 222, 503),
    (65, 'GQ', 'EQUATORIAL GUINEA', 'Equatorial Guinea', 'GNQ', 226, 240),
    (66, 'ER', 'ERITREA', 'Eritrea', 'ERI', 232, 291),
    (67, 'EE', 'ESTONIA', 'Estonia', 'EST', 233, 372),
    (68, 'ET', 'ETHIOPIA', 'Ethiopia', 'ETH', 231, 251),
    (69, 'FK', 'FALKLAND ISLANDS (MALVINAS)', 'Falkland Islands (Malvinas)', 'FLK', 238, 500),
    (70, 'FO', 'FAROE ISLANDS', 'Faroe Islands', 'FRO', 234, 298),
    (71, 'FJ', 'FIJI', 'Fiji', 'FJI', 242, 679),
    (72, 'FI', 'FINLAND', 'Finland', 'FIN', 246, 358),
    (73, 'FR', 'FRANCE', 'France', 'FRA', 250, 33),
    (74, 'GF', 'FRENCH GUIANA', 'French Guiana', 'GUF', 254, 594),
    (75, 'PF', 'FRENCH POLYNESIA', 'French Polynesia', 'PYF', 258, 689),
    (76, 'TF', 'FRENCH SOUTHERN TERRITORIES', 'French Southern Territories', NULL, NULL, 0),
    (77, 'GA', 'GABON', 'Gabon', 'GAB', 266, 241),
    (78, 'GM', 'GAMBIA', 'Gambia', 'GMB', 270, 220),
    (79, 'GE', 'GEORGIA', 'Georgia', 'GEO', 268, 995),
    (80, 'DE', 'GERMANY', 'Germany', 'DEU', 276, 49),
    (81, 'GH', 'GHANA', 'Ghana', 'GHA', 288, 233),
    (82, 'GI', 'GIBRALTAR', 'Gibraltar', 'GIB', 292, 350),
    (83, 'GR', 'GREECE', 'Greece', 'GRC', 300, 30),
    (84, 'GL', 'GREENLAND', 'Greenland', 'GRL', 304, 299),
    (85, 'GD', 'GRENADA', 'Grenada', 'GRD', 308, 1473),
    (86, 'GP', 'GUADELOUPE', 'Guadeloupe', 'GLP', 312, 590),
    (87, 'GU', 'GUAM', 'Guam', 'GUM', 316, 1),
    (88, 'GT', 'GUATEMALA', 'Guatemala', 'GTM', 320, 502),
    (89, 'GN', 'GUINEA', 'Guinea', 'GIN', 324, 224),
    (90, 'GW', 'GUINEA-BISSAU', 'Guinea-Bissau', 'GNB', 624, 245),
    (91, 'GY', 'GUYANA', 'Guyana', 'GUY', 328, 592),
    (92, 'HT', 'HAITI', 'Haiti', 'HTI', 332, 509),
    (93, 'HM', 'HEARD ISLAND AND MCDONALD ISLANDS', 'Heard Island and Mcdonald Islands', NULL, NULL, 0),
    (94, 'VA', 'HOLY SEE (VATICAN CITY STATE)', 'Holy See (Vatican City State)', 'VAT', 336, 39),
    (95, 'HN', 'HONDURAS', 'Honduras', 'HND', 340, 504),
    (96, 'HK', 'HONG KONG', 'Hong Kong', 'HKG', 344, 852),
    (97, 'HU', 'HUNGARY', 'Hungary', 'HUN', 348, 36),
    (98, 'IS', 'ICELAND', 'Iceland', 'ISL', 352, 354),
    (99, 'IN', 'INDIA', 'India', 'IND', 356, 91),
    (100, 'ID', 'INDONESIA', 'Indonesia', 'IDN', 360, 62),
    (101, 'IR', 'IRAN, ISLAMIC REPUBLIC OF', 'Iran, Islamic Republic of', 'IRN', 364, 98),
    (102, 'IQ', 'IRAQ', 'Iraq', 'IRQ', 368, 964),
    (103, 'IE', 'IRELAND', 'Ireland', 'IRL', 372, 353),
    (104, 'IL', 'ISRAEL', 'Israel', 'ISR', 376, 972),
    (105, 'IT', 'ITALY', 'Italy', 'ITA', 380, 39),
    (106, 'JM', 'JAMAICA', 'Jamaica', 'JAM', 388, 1876),
    (107, 'JP', 'JAPAN', 'Japan', 'JPN', 392, 81),
    (108, 'JO', 'JORDAN', 'Jordan', 'JOR', 400, 962),
    (109, 'KZ', 'KAZAKHSTAN', 'Kazakhstan', 'KAZ', 398, 7),
    (110, 'KE', 'KENYA', 'Kenya', 'KEN', 404, 254),
    (111, 'KI', 'KIRIBATI', 'Kiribati', 'KIR', 296, 686),
    (112, 'KP', 'KOREA, DEMOCRATIC PEOPLE''S REPUBLIC OF', 'Korea, Democratic People''s Republic of', 'PRK', 408, 850),
    (113, 'KR', 'KOREA, REPUBLIC OF', 'Korea, Republic of', 'KOR', 410, 82),
    (114, 'KW', 'KUWAIT', 'Kuwait', 'KWT', 414, 965),
    (115, 'KG', 'KYRGYZSTAN', 'Kyrgyzstan', 'KGZ', 417, 996),
    (116, 'LA', 'LAO PEOPLE''S DEMOCRATIC REPUBLIC', 'Lao People''s Democratic Republic', 'LAO', 418, 856),
    (117, 'LV', 'LATVIA', 'Latvia', 'LVA', 428, 371),
    (118, 'LB', 'LEBANON', 'Lebanon', 'LBN', 422, 961),
    (119, 'LS', 'LESOTHO', 'Lesotho', 'LSO', 426, 266),
    (120, 'LR', 'LIBERIA', 'Liberia', 'LBR', 430, 231),
    (121, 'LY', 'LIBYAN ARAB JAMAHIRIYA', 'Libyan Arab Jamahiriya', 'LBY', 434, 218),
    (122, 'LI', 'LIECHTENSTEIN', 'Liechtenstein', 'LIE', 438, 423),
    (123, 'LT', 'LITHUANIA', 'Lithuania', 'LTU', 440, 370),
    (124, 'LU', 'LUXEMBOURG', 'Luxembourg', 'LUX', 442, 352),
    (125, 'MO', 'MACAO', 'Macao', 'MAC', 446, 853),
    (126, 'MK', 'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF', 'Macedonia, the Former Yugoslav Republic of', 'MKD', 807, 389),
    (127, 'MG', 'MADAGASCAR', 'Madagascar', 'MDG', 450, 261),
    (128, 'MW', 'MALAWI', 'Malawi', 'MWI', 454, 265),
    (129, 'MY', 'MALAYSIA', 'Malaysia', 'MYS', 458, 60),
    (130, 'MV', 'MALDIVES', 'Maldives', 'MDV', 462, 960),
    (131, 'ML', 'MALI', 'Mali', 'MLI', 466, 223),
    (132, 'MT', 'MALTA', 'Malta', 'MLT', 470, 356),
    (133, 'MH', 'MARSHALL ISLANDS', 'Marshall Islands', 'MHL', 584, 692),
    (134, 'MQ', 'MARTINIQUE', 'Martinique', 'MTQ', 474, 596),
    (135, 'MR', 'MAURITANIA', 'Mauritania', 'MRT', 478, 222),
    (136, 'MU', 'MAURITIUS', 'Mauritius', 'MUS', 480, 230),
    (137, 'YT', 'MAYOTTE', 'Mayotte', NULL, NULL, 269),
    (138, 'MX', 'MEXICO', 'Mexico', 'MEX', 484, 52),
    (139, 'FM', 'MICRONESIA, FEDERATED STATES OF', 'Micronesia, Federated States of', 'FSM', 583, 691),
    (140, 'MD', 'MOLDOVA, REPUBLIC OF', 'Moldova, Republic of', 'MDA', 498, 373),
    (141, 'MC', 'MONACO', 'Monaco', 'MCO', 492, 377),
    (142, 'MN', 'MONGOLIA', 'Mongolia', 'MNG', 496, 976),
    (143, 'MS', 'MONTSERRAT', 'Montserrat', 'MSR', 500, 1664),
    (144, 'MA', 'MOROCCO', 'Morocco', 'MAR', 504, 212),
    (145, 'MZ', 'MOZAMBIQUE', 'Mozambique', 'MOZ', 508, 258),
    (146, 'MM', 'MYANMAR', 'Myanmar', 'MMR', 104, 95),
    (147, 'NA', 'NAMIBIA', 'Namibia', 'NAM', 516, 264),
    (148, 'NR', 'NAURU', 'Nauru', 'NRU', 520, 674),
    (149, 'NP', 'NEPAL', 'Nepal', 'NPL', 524, 977),
    (150, 'NL', 'NETHERLANDS', 'Netherlands', 'NLD', 528, 31),
    (151, 'AN', 'NETHERLANDS ANTILLES', 'Netherlands Antilles', 'ANT', 530, 599),
    (152, 'NC', 'NEW CALEDONIA', 'New Caledonia', 'NCL', 540, 687),
    (153, 'NZ', 'NEW ZEALAND', 'New Zealand', 'NZL', 554, 64),
    (154, 'NI', 'NICARAGUA', 'Nicaragua', 'NIC', 558, 505),
    (155, 'NE', 'NIGER', 'Niger', 'NER', 562, 227),
    (156, 'NG', 'NIGERIA', 'Nigeria', 'NGA', 566, 234),
    (157, 'NU', 'NIUE', 'Niue', 'NIU', 570, 683),
    (158, 'NF', 'NORFOLK ISLAND', 'Norfolk Island', 'NFK', 574, 672),
    (159, 'MP', 'NORTHERN MARIANA ISLANDS', 'Northern Mariana Islands', 'MNP', 580, 1670),
    (160, 'NO', 'NORWAY', 'Norway', 'NOR', 578, 47),
    (161, 'OM', 'OMAN', 'Oman', 'OMN', 512, 968),
    (162, 'PK', 'PAKISTAN', 'Pakistan', 'PAK', 586, 92),
    (163, 'PW', 'PALAU', 'Palau', 'PLW', 585, 680),
    (164, 'PS', 'PALESTINIAN TERRITORY, OCCUPIED', 'Palestinian Territory, Occupied', NULL, NULL, 970),
    (165, 'PA', 'PANAMA', 'Panama', 'PAN', 591, 507),
    (166, 'PG', 'PAPUA NEW GUINEA', 'Papua New Guinea', 'PNG', 598, 675),
    (167, 'PY', 'PARAGUAY', 'Paraguay', 'PRY', 600, 595),
    (168, 'PE', 'PERU', 'Peru', 'PER', 604, 51),
    (169, 'PH', 'PHILIPPINES', 'Philippines', 'PHL', 608, 63),
    (170, 'PN', 'PITCAIRN', 'Pitcairn', 'PCN', 612, 0),
    (171, 'PL', 'POLAND', 'Poland', 'POL', 616, 48),
    (172, 'PT', 'PORTUGAL', 'Portugal', 'PRT', 620, 351),
    (173, 'PR', 'PUERTO RICO', 'Puerto Rico', 'PRI', 630, 1),
    (174, 'QA', 'QATAR', 'Qatar', 'QAT', 634, 974),
    (175, 'RE', 'REUNION', 'Reunion', 'REU', 638, 262),
    (176, 'RO', 'ROMANIA', 'Romania', 'ROM', 642, 40),
    (177, 'RU', 'RUSSIAN FEDERATION', 'Russian Federation', 'RUS', 643, 70),
    (178, 'RW', 'RWANDA', 'Rwanda', 'RWA', 646, 250),
    (179, 'SH', 'SAINT HELENA', 'Saint Helena', 'SHN', 654, 290),
    (180, 'KN', 'SAINT KITTS AND NEVIS', 'Saint Kitts and Nevis', 'KNA', 659, 1869),
    (181, 'LC', 'SAINT LUCIA', 'Saint Lucia', 'LCA', 662, 1758),
    (182, 'PM', 'SAINT PIERRE AND MIQUELON', 'Saint Pierre and Miquelon', 'SPM', 666, 508),
    (183, 'VC', 'SAINT VINCENT AND THE GRENADINES', 'Saint Vincent and the Grenadines', 'VCT', 670, 1784),
    (184, 'WS', 'SAMOA', 'Samoa', 'WSM', 882, 684),
    (185, 'SM', 'SAN MARINO', 'San Marino', 'SMR', 674, 378),
    (186, 'ST', 'SAO TOME AND PRINCIPE', 'Sao Tome and Principe', 'STP', 678, 239),
    (187, 'SA', 'SAUDI ARABIA', 'Saudi Arabia', 'SAU', 682, 966),
    (188, 'SN', 'SENEGAL', 'Senegal', 'SEN', 686, 221),
    (189, 'CS', 'SERBIA AND MONTENEGRO', 'Serbia and Montenegro', NULL, NULL, 381),
    (190, 'SC', 'SEYCHELLES', 'Seychelles', 'SYC', 690, 248),
    (191, 'SL', 'SIERRA LEONE', 'Sierra Leone', 'SLE', 694, 232),
    (192, 'SG', 'SINGAPORE', 'Singapore', 'SGP', 702, 65),
    (193, 'SK', 'SLOVAKIA', 'Slovakia', 'SVK', 703, 421),
    (194, 'SI', 'SLOVENIA', 'Slovenia', 'SVN', 705, 386),
    (195, 'SB', 'SOLOMON ISLANDS', 'Solomon Islands', 'SLB', 90, 677),
    (196, 'SO', 'SOMALIA', 'Somalia', 'SOM', 706, 252),
    (197, 'ZA', 'SOUTH AFRICA', 'South Africa', 'ZAF', 710, 27),
    (198, 'GS', 'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS', 'South Georgia and the South Sandwich Islands', NULL, NULL, 0),
    (199, 'ES', 'SPAIN', 'Spain', 'ESP', 724, 34),
    (200, 'LK', 'SRI LANKA', 'Sri Lanka', 'LKA', 144, 94),
    (201, 'SD', 'SUDAN', 'Sudan', 'SDN', 736, 249),
    (202, 'SR', 'SURINAME', 'Suriname', 'SUR', 740, 597),
    (203, 'SJ', 'SVALBARD AND JAN MAYEN', 'Svalbard and Jan Mayen', 'SJM', 744, 47),
    (204, 'SZ', 'SWAZILAND', 'Swaziland', 'SWZ', 748, 268),
    (205, 'SE', 'SWEDEN', 'Sweden', 'SWE', 752, 46),
    (206, 'CH', 'SWITZERLAND', 'Switzerland', 'CHE', 756, 41),
    (207, 'SY', 'SYRIAN ARAB REPUBLIC', 'Syrian Arab Republic', 'SYR', 760, 963),
    (208, 'TW', 'TAIWAN, PROVINCE OF CHINA', 'Taiwan, Province of China', 'TWN', 158, 886),
    (209, 'TJ', 'TAJIKISTAN', 'Tajikistan', 'TJK', 762, 992),
    (210, 'TZ', 'TANZANIA, UNITED REPUBLIC OF', 'Tanzania, United Republic of', 'TZA', 834, 255),
    (211, 'TH', 'THAILAND', 'Thailand', 'THA', 764, 66),
    (212, 'TL', 'TIMOR-LESTE', 'Timor-Leste', NULL, NULL, 670),
    (213, 'TG', 'TOGO', 'Togo', 'TGO', 768, 228),
    (214, 'TK', 'TOKELAU', 'Tokelau', 'TKL', 772, 690),
    (215, 'TO', 'TONGA', 'Tonga', 'TON', 776, 676),
    (216, 'TT', 'TRINIDAD AND TOBAGO', 'Trinidad and Tobago', 'TTO', 780, 1868),
    (217, 'TN', 'TUNISIA', 'Tunisia', 'TUN', 788, 216),
    (218, 'TR', 'TURKEY', 'Turkey', 'TUR', 792, 90),
    (219, 'TM', 'TURKMENISTAN', 'Turkmenistan', 'TKM', 795, 7370),
    (220, 'TC', 'TURKS AND CAICOS ISLANDS', 'Turks and Caicos Islands', 'TCA', 796, 1649),
    (221, 'TV', 'TUVALU', 'Tuvalu', 'TUV', 798, 688),
    (222, 'UG', 'UGANDA', 'Uganda', 'UGA', 800, 256),
    (223, 'UA', 'UKRAINE', 'Ukraine', 'UKR', 804, 380),
    (224, 'AE', 'UNITED ARAB EMIRATES', 'United Arab Emirates', 'ARE', 784, 971),
    (225, 'GB', 'UNITED KINGDOM', 'United Kingdom', 'GBR', 826, 44),
    (226, 'US', 'UNITED STATES', 'United States', 'USA', 840, 1),
    (227, 'UM', 'UNITED STATES MINOR OUTLYING ISLANDS', 'United States Minor Outlying Islands', NULL, NULL, 1),
    (228, 'UY', 'URUGUAY', 'Uruguay', 'URY', 858, 598),
    (229, 'UZ', 'UZBEKISTAN', 'Uzbekistan', 'UZB', 860, 998),
    (230, 'VU', 'VANUATU', 'Vanuatu', 'VUT', 548, 678),
    (231, 'VE', 'VENEZUELA', 'Venezuela', 'VEN', 862, 58),
    (232, 'VN', 'VIETNAM', 'Vietnam', 'VNM', 704, 84),
    (233, 'VG', 'VIRGIN ISLANDS, BRITISH', 'Virgin Islands, British', 'VGB', 92, 1),
    (234, 'VI', 'VIRGIN ISLANDS, U.S.', 'Virgin Islands, US', 'VIR', 850, 1),
    (235, 'WF', 'WALLIS AND FUTUNA', 'Wallis and Futuna', 'WLF', 876, 681),
    (236, 'EH', 'WESTERN SAHARA', 'Western Sahara', 'ESH', 732, 212),
    (237, 'YE', 'YEMEN', 'Yemen', 'YEM', 887, 967),
    (238, 'ZM', 'ZAMBIA', 'Zambia', 'ZMB', 894, 260),
    (239, 'ZW', 'ZIMBABWE', 'Zimbabwe', 'ZWE', 716, 263);



    create table #country_format  (
        country varchar(50),
        nicename varchar(50),
        iso varchar(3),
        phonecode varchar(50)
        );
    insert into #country_format 
    (	country,
        nicename,
        iso,
        phonecode
        )
    select country
        ,nicename 
        , iso 
        , phonecode
    from 
    (
    select country 
     , case when len(country) = 2 then country 
        else null 
        end as country_code
    , case when len(country) >= 3 then country 
        else null 
        end as country_string
    from 
        (select distinct case when country in ('Virgin Islands', 'US Virgin Islands', 'Virgin Islands (U.S.)') then 'Virgin Islands, US'
                when country = 'Canda' then 'Canada'
                when country = 'Netherlands, The' then 'Netherlands'
                when country in ('USA', 'US`') then 'United States'
                else trim(Country)
                end as country
            FROM [Clarkitbiz_prod].[dbo].[tblOrderShipping]
            where country not like '%[0-9]%'
                and country != '') as t1) as a
    left join 
    (select *
    from #country_list) as b
        on a.country_string = b.nicename
        or a.country_code = b.iso;

    ----------------------------------------------------------------------------------------------------------------------------------;
    --drop table #all_orders;

    create table #all_orders (
        user_email varchar(100),
        firstname varchar(100),
        lastname varchar(100),
        zip varchar(100),
        country_iso varchar(100),
        international_phone varchar(100),
        RFM varchar(100),
        rfm_category varchar(100)
    );


    with all_orders_cte as (
    SELECT distinct 
        a.user_email
        , lower(trim(Ltrim(SubString(b.name, 1, Isnull(Nullif(CHARINDEX(' ', b.name), 0), 1000))))) AS FirstName
        --, lower(Ltrim(SUBSTRING(name, CharIndex(' ', name), 
        --	CASE WHEN (CHARINDEX(' ', name, CHARINDEX(' ', name) + 1) - CHARINDEX(' ', name)) <= 0 THEN 0
        --           ELSE CHARINDEX(' ', name, CHARINDEX(' ', name) + 1) - CHARINDEX(' ', name)
        --           END))) AS MiddleName
        , lower(trim(Ltrim(SUBSTRING(b.name, Isnull(Nullif(CHARINDEX(' ', b.name, Charindex(' ', b.name) + 1), 0), CHARINDEX(' ', b.name)), 
            CASE WHEN Charindex(' ', b.name) = 0 THEN 0
                ELSE LEN(b.name)
                END)))) AS LastName
        , CASE WHEN charindex('EXT', b.phone) > 0 THEN substring(trim(b.phone), 1, charindex('EXT', b.phone)-1) 
            WHEN charindex('EX', b.phone) > 0 THEN substring(trim(b.phone), 1, charindex('EX', b.phone)-1) 
            WHEN charindex('X', b.phone) > 0 THEN substring(trim(b.phone), 1, charindex('X', b.phone)-1) 
            WHEN charindex(';', b.phone) > 0 THEN substring(trim(b.phone), 1, charindex(';', b.phone)-1) 
            ELSE b.phone end as phone
        , b.zip
        , c.iso as country_iso
        , c.phonecode
        , CASE 
            WHEN d.diff_date <= 60 THEN '4' 
            WHEN d.diff_date <= 90 THEN '3' 
            WHEN d.diff_date <= 180 THEN '2' 
            ELSE '1' 
            END AS r_recency
        , CASE 
                WHEN d.count_order >= 10 THEN '4' 
                WHEN d.count_order >= 7 THEN '3' 
                WHEN d.count_order >= 4 THEN '2' 
                ELSE '1' 
            END AS r_freq
        ,    CASE 
                WHEN d.total_amount >= 2000 THEN '4' 
                WHEN d.total_amount >= 1000 THEN '3' 
                WHEN d.total_amount >= 500 THEN '2' 
                ELSE '1' 
            END AS r_monetary
      FROM [Clarkitbiz_prod].[dbo].[tblOrders] as a
      inner join 
        (select trim(name) as name
            , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone, 
            '+', ''),
            '-', ''),
            ' ', ''),
            '(', ''),
            '*', ''),
            '.', ''),
            '`', ''),
            '/', ''),
            '\', ''),
            'O', '0'),
            ')', '') as phone
            , Zip
            , Country
            , order_number
        FROM [Clarkitbiz_prod].[dbo].[tblOrderShipping]) as b
        on a.order_number = b.order_number
        left join #country_format as c 
            on b.country = c.country
        LEFT JOIN 
        (SELECT user_email, 
                Count(*)  AS count_order, 
                Sum([total])  AS total_amount, 
                Datediff(day, Max([date_ordered]), Getdate()) AS diff_date 
            FROM [Clarkitbiz_prod].[dbo].[tblorders] 
            WHERE status != 'Canceled'
                and customer_id = 'WEB100'
                and customer_location = 851
                and subtotal > 0
            GROUP BY user_email) as d
        on a.user_email = d.user_email
        where year(a.date_ordered) >= {year_filter}
        and month(a.date_ordered) >= {month_filter}
        and a.status != 'Canceled'
        and a.customer_id = 'WEB100'
        and a.customer_location = 851
        and a.subtotal > 0
        )
    , add_rfm_cte as (
        select *
            , CASE WHEN substring(phone, 1, len(phonecode)) != phonecode and len(phonecode) = 1 then '+' + substring(phonecode + phone, 1, 11)
                WHEN substring(phone, 1, len(phonecode)) != phonecode and len(phonecode) = 2 then '+' + substring(phonecode + phone, 1, 12)
                WHEN substring(phone, 1, len(phonecode)) != phonecode and len(phonecode) = 3 then '+' + substring(phonecode + phone, 1, 13)
                WHEN substring(phone, 1, len(phonecode)) != phonecode and len(phonecode) = 4 then '+' + substring(phonecode + phone, 1, 14)
                ELSE '+' + phone
                END AS international_phone
            , r_recency + r_freq + r_monetary as RFM
        FROM all_orders_cte)
    , rfm_cte as (
        SELECT * 
            , CASE WHEN RFM in ('444') then 'Loyal Customers'
                WHEN RFM in ('423', '424', '433', '434', '443', '543') then 'Potential Loyalist'
                WHEN RFM in ('442', '432', '344', '334', '422', '441', '431', '421') then 'Promising Customers'
                WHEN RFM in ('412', '411', '413', '414') then 'New Customers'
                WHEN RFM in ('322', '323', '324', '332', '331', '312', '313', '314', 
                    '342', '343', '333', '341', '311', '321', '233', '243', '234', '244') then 'Needing Attention'
                WHEN RFM in ('222', '221', '223', '224', '231', 
                    '232', '134', '144', '143', '133', '241', '242', '211', '212', '213', '214') then 'At Risk'
                WHEN RFM in ('112', '113', '114', '121', '122', 
                    '123', '124', '131', '132', '141', '142', '111') then 'Lost'
                END AS RFM_category
        FROM add_rfm_cte)
    insert into #all_orders (
        user_email,
        firstname,
        lastname,
        zip,
        country_iso,
        international_phone,
        RFM,
        rfm_category)
    select 	
        user_email,
        firstname,
        lastname,
        zip,
        country_iso,
        international_phone,
        RFM,
        rfm_category
    from rfm_cte;

    select *
    from #all_orders;
    '''.format(month_filter=month_filter, year_filter=year_filter)

    # execute sql query
    df = pd.read_sql_query(customer_list_sql, conn)

    # remove duplicates
    df = df.drop_duplicates()

    return df


def customer_match_df(df):
    """Takes in a customer list dataframe output from pull_user_list function.
    Selects only columns needed for customer match.
    Creates a separate dataframe for each rfm category
    """
    # create copy and select only columns needed
    df_customer_list_narrow = df.copy()[['user_email', 'firstname', 'lastname', 'country_iso', 'zip',
                                         'international_phone', 'rfm_category']]

    # rename variable names
    df_customer_list_narrow.rename(columns={'user_email': 'Email',
                                            'firstname': 'First Name',
                                            'lastname': 'Last Name',
                                            'country_iso': 'Country',
                                            'zip': 'Zip',
                                            'international_phone': 'Phone'}, inplace=True)

    # look at count of records per RFM category
    print(df_customer_list_narrow.groupby('rfm_category').count())

    # create list of rfm category names
    rfm_list = df['rfm_category'].unique().tolist()

    # create a dataframe for each rfm category name
    DataFrameDict = {elem: pd.DataFrame for elem in rfm_list}

    # insert all data related to each dictionary name
    for key in DataFrameDict.keys():
        DataFrameDict[key] = df.iloc[:, :-1][df.rfm_category == key]

    return rfm_list, DataFrameDict


def output_rfm_to_gbq(df, rfm_list):
    """Takes in a customer list dataframe output from pull_user_list function.
    Then creates/updates a table on Google Big Query.
    Each table contains the customer information (user_email, first_name, last_name, phone, zip, country and rfm)
    """

    # create a dataframe for each rfm category name
    CustomerMatchDict = {elem: pd.DataFrame for elem in rfm_list}

    # insert all data related to each dictionary name
    for key in CustomerMatchDict.keys():
        CustomerMatchDict[key] = df.iloc[:, :-1][df.rfm_category == key]

    # upload to gbq
    for rfm in rfm_list:
        df = CustomerMatchDict[rfm]
        rfm = rfm.replace(' ', '_')
        path_index = "customer_match.%(rfm_name)s" % {'rfm_name': rfm}
        # write to dataset
        df.to_gbq(path_index, "hidden-moon-164616", credentials=credentials, if_exists="replace", chunksize=100000)


