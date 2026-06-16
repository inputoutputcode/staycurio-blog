const makeId = (src: string) =>
	src
		.split('/')
		.at(-1)
		?.replace(/\.jpg$/, '')
		.toLowerCase()
		.replaceAll('_', '-')
		.replaceAll('.', '-') ?? '';

export const galleryGroups = [
	{
		title: 'Member Experience',
		description: 'The public journey from landing page to signup, chat, matches, settings, and feedback.',
		images: [
			['/real-life-friends/Web/01_Landing_Page.jpg', 'Landing page'],
			['/real-life-friends/Web/70_LandingPage_Short.jpg', 'Short landing page'],
			['/real-life-friends/Web/71_LandingPage_Journey.jpg', 'Landing page journey'],
			['/real-life-friends/Web/02_Signup_Step1.jpg', 'Signup step one'],
			['/real-life-friends/Web/03_Signup_Step2.jpg', 'Signup step two'],
			['/real-life-friends/Web/08_Chat_Welcome.jpg', 'Chat welcome'],
			['/real-life-friends/Web/09_Chat_Extraction_Flow.jpg', 'Chat extraction flow'],
			['/real-life-friends/Web/10_Chat_MatchReady.jpg', 'Match ready chat'],
			['/real-life-friends/Web/11_Your_Matches.jpg', 'Your matches'],
			['/real-life-friends/Web/06_Settings.jpg', 'Settings'],
			['/real-life-friends/Web/07_Feeback.jpg', 'Feedback'],
		],
	},
	{
		title: 'Perks and Partners',
		description: 'Partner onboarding, perk review, perk acceptance, and QR code redemption.',
		images: [
			['/real-life-friends/Web/40_Matches_Pick_Perk.jpg', 'Pick a perk'],
			['/real-life-friends/Web/41_Matches_Review_Perk.jpg', 'Review perk'],
			['/real-life-friends/Web/42_Matches_Suggested_Perk.jpg', 'Suggested perk'],
			['/real-life-friends/Web/43_Matches_Accepted_Perk.jpg', 'Accepted perk'],
			['/real-life-friends/Web/44_Partner_Portal_Management-UI.jpg', 'Partner portal management'],
			['/real-life-friends/Web/45_Partner_Portal_Login.jpg', 'Partner portal login'],
			['/real-life-friends/Web/46_Partner_Portal_Apply.jpg', 'Partner portal application'],
			['/real-life-friends/Web/48_QR-Code_Valid.jpg', 'Valid QR code'],
			['/real-life-friends/Web/49_QR-Code_Already_Reedemed.jpg', 'Redeemed QR code'],
		],
	},
	{
		title: 'Admin Console',
		description: 'Operational views for dashboards, match review, users, analytics, risks, and moderation.',
		images: [
			['/real-life-friends/Web/20_Admin_Dashboard.jpg', 'Admin dashboard'],
			['/real-life-friends/Web/21_Admin_Matches.jpg', 'Admin matches'],
			['/real-life-friends/Web/22_Admin_Feedback.jpg', 'Admin feedback'],
			['/real-life-friends/Web/23_Admin_Analytics.jpg', 'Admin analytics'],
			['/real-life-friends/Web/24_Admin_Risks.jpg', 'Admin risks'],
			['/real-life-friends/Web/25_Admin_User.jpg', 'Admin user'],
			['/real-life-friends/Web/26_Admin_Users.jpg', 'Admin users'],
			['/real-life-friends/Web/27_Admin_Performance.jpg', 'Admin performance'],
			['/real-life-friends/Web/81_Admin_Scoring_Config.jpg', 'Admin scoring config'],
			['/real-life-friends/Web/82_Admin_Graph.jpg', 'Admin graph'],
			['/real-life-friends/Web/83_Admin_Canonicals_Tags.jpg', 'Canonical tags'],
			['/real-life-friends/Web/84_Admin_Canonicals_Merge.jpg', 'Canonical merge'],
			['/real-life-friends/Web/85_Admin_Matches_Reasoning.jpg', 'Match reasoning'],
			['/real-life-friends/Web/86_Admin_Perks_Application_Approval.jpg', 'Perk application approval'],
			['/real-life-friends/Web/86_Admin_Perks_Stats.jpg', 'Perk stats'],
			['/real-life-friends/Web/87_Admin_Feature_Flag.jpg', 'Feature flags'],
			['/real-life-friends/Web/88_Admin_Matches_Reasoning_v1.jpg', 'Match reasoning v1'],
			['/real-life-friends/Web/88_Admin_Matches_Reasoning_v2.jpg', 'Match reasoning v2'],
			['/real-life-friends/Web/88_Admin_Matches_Reasoning_v3.jpg', 'Match reasoning v3'],
		],
	},
	{
		title: 'Evaluation and Reliability',
		description: 'Testing, prompt tuning, scoring, mobile web checks, and infrastructure visibility.',
		images: [
			['/real-life-friends/Web/50_User_Chat_Mobile_View.jpg', 'User chat mobile web'],
			['/real-life-friends/Web/51_Admin_Dashboard_Mobile_View.jpg', 'Admin dashboard mobile web'],
			['/real-life-friends/Web/63_Grafana_Node.jpg', 'Grafana node dashboard'],
			['/real-life-friends/Web/90_Test_Dashboard.jpg', 'Test dashboard'],
			['/real-life-friends/Web/91_Test_Core_Eval_Result.jpg', 'Core eval result'],
			['/real-life-friends/Web/92_Test_Scoring_Playground.jpg', 'Scoring playground'],
			['/real-life-friends/Web/93_Test_Prompt_Tuning_Result.jpg', 'Prompt tuning result'],
		],
	},
	{
		title: 'Native Mobile',
		description: 'iOS and Android screens for login, chat, matches, settings, and feedback.',
		images: [
			['/real-life-friends/Mobile/00_iOS_Login.jpg', 'iOS login'],
			['/real-life-friends/Mobile/01_iOS_Chat.jpg', 'iOS chat'],
			['/real-life-friends/Mobile/02_iOS_Matches.jpg', 'iOS matches'],
			['/real-life-friends/Mobile/03_iOS_Feedback.jpg', 'iOS feedback'],
			['/real-life-friends/Mobile/04_iOS_Settings.jpg', 'iOS settings'],
			['/real-life-friends/Mobile/10_Android_Chat.jpg', 'Android chat'],
			['/real-life-friends/Mobile/11_Android_Feedback.jpg', 'Android feedback'],
			['/real-life-friends/Mobile/11_Android_Matches.jpg', 'Android matches'],
			['/real-life-friends/Mobile/11_Android_Settings.jpg', 'Android settings'],
		],
	},
] as const;

export const galleryItems = galleryGroups.flatMap((group) =>
	group.images.map(([src, caption]) => ({
		id: makeId(src),
		src,
		caption,
		groupTitle: group.title,
	})),
);
