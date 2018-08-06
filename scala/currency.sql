BEGIN TRANSACTION READ WRITE;

DROP TABLE IF EXISTS "public"."currency";

CREATE TABLE "public"."currency" (
    "alphabetic_code" VARCHAR(4) NOT NULL UNIQUE,
    "currency" VARCHAR(128) NOT NULL UNIQUE
) COMPOUND SORTKEY("alphabetic_code", "currency");

INSERT INTO "public"."currency" ("alphabetic_code", "currency") VALUES
  ('AED', 'United Arab Emirates dirham'),
  ('AFN', 'Afghan afghani'),
  ('ALL', 'Albanian lek'),
  ('AMD', 'Armenian dram'),
  ('ANG', 'Netherlands Antillean guilder'),
  ('AOA', 'Angolan kwanza'),
  ('ARS', 'Argentine peso'),
  ('AUD', 'Australian dollar'),
  ('AWG', 'Aruban florin'),
  ('AZN', 'Azerbaijani manat'),
  ('BAM', 'Bosnia and Herzegovina convertible mark'),
  ('BBD', 'Barbados dollar'),
  ('BDT', 'Bangladeshi taka'),
  ('BGN', 'Bulgarian lev'),
  ('BHD', 'Bahraini dinar'),
  ('BIF', 'Burundian franc'),
  ('BMD', 'Bermudian dollar'),
  ('BND', 'Brunei dollar'),
  ('BOB', 'Boliviano'),
  ('BRL', 'Brazilian real'),
  ('BSD', 'Bahamian dollar'),
  ('BTN', 'Bhutanese ngultrum'),
  ('BWP', 'Botswana pula'),
  ('BYN', 'Belarusian ruble'),
  ('BZD', 'Belize dollar'),
  ('CAD', 'Canadian dollar'),
  ('CDF', 'Congolese franc'),
  ('CHF', 'Swiss franc'),
  ('CLP', 'Chilean peso'),
  ('CNY', 'Renminbi (Chinese) yuan'),
  ('COP', 'Colombian peso'),
  ('CRC', 'Costa Rican colon'),
  ('CUC', 'Cuban convertible peso'),
  ('CUP', 'Cuban peso'),
  ('CVE', 'Cape Verde escudo'),
  ('CZK', 'Czech koruna'),
  ('DJF', 'Djiboutian franc'),
  ('DKK', 'Danish krone'),
  ('DOP', 'Dominican peso'),
  ('DZD', 'Algerian dinar'),
  ('EGP', 'Egyptian pound'),
  ('ERN', 'Eritrean nakfa'),
  ('ETB', 'Ethiopian birr'),
  ('EUR', 'Euro'),
  ('FJD', 'Fiji dollar'),
  ('FKP', 'Falkland Islands pound'),
  ('GBP', 'Pound sterling'),
  ('GEL', 'Georgian lari'),
  ('GHS', 'Ghanaian cedi'),
  ('GIP', 'Gibraltar pound'),
  ('GMD', 'Gambian dalasi'),
  ('GNF', 'Guinean franc'),
  ('GTQ', 'Guatemalan quetzal'),
  ('GYD', 'Guyanese dollar'),
  ('HKD', 'Hong Kong dollar'),
  ('HNL', 'Honduran lempira'),
  ('HRK', 'Croatian kuna'),
  ('HTG', 'Haitian gourde'),
  ('HUF', 'Hungarian forint'),
  ('IDR', 'Indonesian rupiah'),
  ('ILS', 'Israeli new shekel'),
  ('INR', 'Indian rupee'),
  ('IQD', 'Iraqi dinar'),
  ('IRR', 'Iranian rial'),
  ('ISK', 'Icelandic króna'),
  ('JMD', 'Jamaican dollar'),
  ('JOD', 'Jordanian dinar'),
  ('JPY', 'Japanese yen'),
  ('KES', 'Kenyan shilling'),
  ('KGS', 'Kyrgyzstani som'),
  ('KHR', 'Cambodian riel'),
  ('KMF', 'Comoro franc'),
  ('KPW', 'North Korean won'),
  ('KRW', 'South Korean won'),
  ('KWD', 'Kuwaiti dinar'),
  ('KYD', 'Cayman Islands dollar'),
  ('KZT', 'Kazakhstani tenge'),
  ('LAK', 'Lao kip'),
  ('LBP', 'Lebanese pound'),
  ('LKR', 'Sri Lankan rupee'),
  ('LRD', 'Liberian dollar'),
  ('LSL', 'Lesotho loti'),
  ('LYD', 'Libyan dinar'),
  ('MAD', 'Moroccan dirham'),
  ('MDL', 'Moldovan leu'),
  ('MGA', 'Malagasy ariary'),
  ('MKD', 'Macedonian denar'),
  ('MMK', 'Myanmar kyat'),
  ('MNT', 'Mongolian tögrög'),
  ('MOP', 'Macanese pataca'),
  ('MRU', 'Mauritanian ouguiya'),
  ('MUR', 'Mauritian rupee'),
  ('MVR', 'Maldivian rufiyaa'),
  ('MWK', 'Malawian kwacha'),
  ('MXN', 'Mexican peso'),
  ('MYR', 'Malaysian ringgit'),
  ('MZN', 'Mozambican metical'),
  ('NAD', 'Namibian dollar'),
  ('NGN', 'Nigerian naira'),
  ('NIO', 'Nicaraguan córdoba'),
  ('NOK', 'Norwegian krone'),
  ('NPR', 'Nepalese rupee'),
  ('NZD', 'New Zealand dollar'),
  ('OMR', 'Omani rial'),
  ('PAB', 'Panamanian balboa'),
  ('PEN', 'Peruvian sol'),
  ('PGK', 'Papua New Guinean kina'),
  ('PHP', 'Philippine piso'),
  ('PKR', 'Pakistani rupee'),
  ('PLN', 'Polish złoty'),
  ('PYG', 'Paraguayan guaraní'),
  ('QAR', 'Qatari riyal'),
  ('RON', 'Romanian leu'),
  ('RSD', 'Serbian dinar'),
  ('RUB', 'Russian ruble'),
  ('RWF', 'Rwandan franc'),
  ('SAR', 'Saudi riyal'),
  ('SBD', 'Solomon Islands dollar'),
  ('SCR', 'Seychelles rupee'),
  ('SDG', 'Sudanese pound'),
  ('SEK', 'Swedish krona/kronor'),
  ('SGD', 'Singapore dollar'),
  ('SHP', 'Saint Helena pound'),
  ('SLL', 'Sierra Leonean leone'),
  ('SOS', 'Somali shilling'),
  ('SRD', 'Surinamese dollar'),
  ('SSP', 'South Sudanese pound'),
  ('STN', 'São Tomé and Príncipe dobra'),
  ('SVC', 'Salvadoran colón'),
  ('SYP', 'Syrian pound'),
  ('SZL', 'Swazi lilangeni'),
  ('THB', 'Thai baht'),
  ('TJS', 'Tajikistani somoni'),
  ('TMT', 'Turkmenistan manat'),
  ('TND', 'Tunisian dinar'),
  ('TOP', 'Tongan paʻanga'),
  ('TRY', 'Turkish lira'),
  ('TTD', 'Trinidad and Tobago dollar'),
  ('TWD', 'New Taiwan dollar'),
  ('TZS', 'Tanzanian shilling'),
  ('UAH', 'Ukrainian hryvnia'),
  ('UGX', 'Ugandan shilling'),
  ('USD', 'United States dollar'),
  ('UYU', 'Uruguayan peso'),
  ('UZS', 'Uzbekistan som'),
  ('VEF', 'Venezuelan bolívar'),
  ('VND', 'Vietnamese đồng'),
  ('VUV', 'Vanuatu vatu'),
  ('WST', 'Samoan tala'),
  ('XAF', 'CFA franc BEAC'),
  ('XCD', 'East Caribbean dollar'),
  ('XOF', 'CFA franc BCEAO'),
  ('XPF', 'CFP franc (franc Pacifique)'),
  ('XTS', 'Code reserved for testing purposes'),
  ('XUA', 'ADB Unit of Account'),
  ('XXX', 'No currency'),
  ('YER', 'Yemeni rial'),
  ('ZAR', 'South African rand'),
  ('ZMW', 'Zambian kwacha'),
  ('ZWL', 'Zimbabwean dollar');

-- --- Cryptocurrencies
--   ('DASH', 'Dash'),
--   ('ETH', 'Ether'),
--   ('VTC', 'Vertcoin'),
--   ('XBC', 'Bitcoin Cash'),
--   ('XBT', 'Bitcoin'),
--   ('XLM', 'Stellar Lumen'),
--   ('XMR', 'Monero'),
--   ('XRP', 'Ripple'),
--   ('ZEC', 'Zcash'),

--- Complementary Currency
--   ('CHW', 'WIR Franc (complementary currency)'),
--   ('CHE', 'WIR Euro (complementary currency)'),
--

--- Funds code
--   ('MXV', 'Mexican Unidad de Inversion (UDI) (funds code)'),
--   ('CLF', 'Unidad de Fomento (funds code)'),
--   ('COU', 'Unidad de Valor Real (UVR) (funds code)[9]'),
--   ('BOV', 'Bolivian Mvdol (funds code)'),
--   ('USN', 'United States dollar (next day) (funds code)'),
--   ('UYI', 'Uruguay Peso en Unidades Indexadas (URUIURUI) (funds code)'),
--   ('XSU', 'SUCRE'),

--- Metals
--   ('XPT', 'Platinum (one troy ounce)'),
--   ('XPD', 'Palladium (one troy ounce)'),
--   ('XAG', 'Silver (one troy ounce)'),
--   ('XAU', 'Gold (one troy ounce)'),

--- Bond market
--   ('XBA', 'European Composite Unit (EURCO) (bond market unit)'),
--   ('XBB', 'European Monetary Unit (E.M.U.-6) (bond market unit)'),
--   ('XBC', 'European Unit of Account 9 (E.U.A.-9) (bond market unit)'),
--   ('XBD', 'European Unit of Account 17 (E.U.A.-17) (bond market unit)'),

--- IMF
--   ('XDR', 'Special drawing rights');
COMMIT;