﻿--DROP TABLE ODS_BEKTAS.HARCAMALISTESI;

DELETE FROM VODAFONE.ELO_TABLES WHERE NAME = 'STG_MIND.BILLING_PERIODS';

DELETE FROM VODAFONE.ELO_COLUMNS WHERE NAME = 'STG_MIND.BILLING_PERIODS';

commit;