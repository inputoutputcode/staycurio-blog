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
		title: 'Member Experience (User)',
		description: 'The user-facing journey from landing pages to signup, chat, matching, settings, perks, and dark mode.',
		images: [
			['/real-life-friends/Web/00_User_LandingPage_Short.jpg', 'Landing page short view'],
			['/real-life-friends/Web/01_User_LandingPage_Journey.jpg', 'Landing page journey'],
			['/real-life-friends/Web/01_User_Landing_Page_Original.jpg', 'Original landing page'],
			['/real-life-friends/Web/02_User_Signup_Step1.jpg', 'Signup step one'],
			['/real-life-friends/Web/03_User_Signup_Step2.jpg', 'Signup step two'],
			['/real-life-friends/Web/04_User_Chat_Welcome.jpg', 'Chat welcome'],
			['/real-life-friends/Web/05_User_Chat_Extraction_Flow.jpg', 'Chat extraction flow'],
			['/real-life-friends/Web/11_User_Your_Matches.jpg', 'Your matches'],
			['/real-life-friends/Web/06_User_Settings.jpg', 'Settings'],
			['/real-life-friends/Web/07_User_Feeback.jpg', 'Feedback'],
			['/real-life-friends/Mobile/18_Mobile_User_Chat_View.jpg', 'User chat mobile web'],
			['/real-life-friends/Web/19_User_Chat_Dark.jpg', 'User chat dark mode'],
		],
	},
	{
		title: 'Admin Console',
		description: 'Operational views for dashboards, feedback, analytics, risks, users, scoring, graph data, canonicals, match reasoning, health, notifications, observability, and feature flags.',
		images: [
			['/real-life-friends/Web/20_Admin_Dashboard.jpg', 'Dashboard'],
			['/real-life-friends/Web/22_Admin_Feedback.jpg', 'Feedback'],
			['/real-life-friends/Web/23_Admin_Analytics.jpg', 'Analytics'],
			['/real-life-friends/Web/24_Admin_Risks.jpg', 'Risks'],
			['/real-life-friends/Web/25_Admin_User.jpg', 'User'],
			['/real-life-friends/Web/26_Admin_Users.jpg', 'Users'],
			['/real-life-friends/Web/27_Admin_Performance.jpg', 'Performance'],
			['/real-life-friends/Web/28_Admin_Health.jpg', 'Health'],
			['/real-life-friends/Web/29_Admin_Scoring_Config.jpg', 'Scoring config'],
			['/real-life-friends/Web/30_Admin_Graph.jpg', 'Graph'],
			['/real-life-friends/Web/31_Admin_Canonicals_Tags.jpg', 'Canonical tags'],
			['/real-life-friends/Web/32_Admin_Canonicals_Merge.jpg', 'Canonical merge'],
			['/real-life-friends/Web/33_Admin_Matches_Reasoning.jpg', 'Match reasoning'],
			['/real-life-friends/Web/34_Admin_Perks_Stats.jpg', 'Perk stats'],
			['/real-life-friends/Web/35_Admin_Feature_Flag.jpg', 'Feature flags'],
			['/real-life-friends/Web/36_Admin_Notifications.jpg', 'Notifications'],
			['/real-life-friends/Web/37_Admin_Notification_Edit.jpg', 'Notification editor'],
			['/real-life-friends/Mobile/38_Mobile_Admin_Dashboard_View.jpg', 'Dashboard mobile web'],
			['/real-life-friends/Web/39_Admin_Dashboard_Dark.jpg', 'Dashboard dark mode'],
			['/real-life-friends/Web/40_Admin_Grafana_Node.jpg', 'Grafana node dashboard'],
			['/real-life-friends/Web/41_Admin_Grafana_Docker_Overview.jpg', 'Grafana Docker overview'],
		],
	},
	{
		title: 'Perks and Partners',
		description: 'User perk selection, QR code redemption, and partner portal flows.',
		images: [
			['/real-life-friends/Web/12_User_Matches_Pick_Perk.jpg', 'Pick a perk'],
			['/real-life-friends/Web/13_User_Matches_Review_Perk.jpg', 'Review perk'],
			['/real-life-friends/Web/14_User_Matches_Suggested_Perk.jpg', 'Suggested perk'],
			['/real-life-friends/Web/15_User_Matches_Accepted_Perk.jpg', 'Accepted perk'],
			['/real-life-friends/Web/16_User_QR-Code_Valid.jpg', 'Valid QR code'],
			['/real-life-friends/Web/17_User_QR-Code_Already_Reedemed.jpg', 'Redeemed QR code'],
			['/real-life-friends/Web/50_Partner_Portal_Login.jpg', 'Partner portal login'],
			['/real-life-friends/Web/51_Partner_Portal_Apply.jpg', 'Partner portal application'],
			['/real-life-friends/Web/52_Partner_Portal_Management-UI.jpg', 'Partner portal management'],
		],
	},
	{
		title: 'Evaluation and Reliability',
		description: 'Testing, prompt tuning, synthetic data, and reliability tooling.',
		images: [
			['/real-life-friends/Web/42_Test_Dashboard.jpg', 'Test dashboard'],
			['/real-life-friends/Web/43_Test_Core_Eval_Result.jpg', 'Core eval result'],
			['/real-life-friends/Web/44_Test_Scoring_Playground.jpg', 'Scoring playground'],
			['/real-life-friends/Web/45_Test_Prompt_Tuning_Result.jpg', 'Prompt tuning result'],
			['/real-life-friends/Web/46_Test_SynthData.jpg', 'Synthetic data'],
		],
	},
	{
		title: 'Native Mobile',
		description: 'iOS and Android screens for login, chat, matches, settings, and feedback.',
		images: [
			['/real-life-friends/Mobile/00_Mobile_iOS_Login.jpg', 'iOS login'],
			['/real-life-friends/Mobile/01_Mobile_iOS_Chat.jpg', 'iOS chat'],
			['/real-life-friends/Mobile/02_Mobile_iOS_Matches.jpg', 'iOS matches'],
			['/real-life-friends/Mobile/03_Mobile_iOS_Feedback.jpg', 'iOS feedback'],
			['/real-life-friends/Mobile/04_Mobile_iOS_Settings.jpg', 'iOS settings'],
			['/real-life-friends/Mobile/10_Mobile_Android_Chat.jpg', 'Android chat'],
			['/real-life-friends/Mobile/11_Mobile_Android_Feedback.jpg', 'Android feedback'],
			['/real-life-friends/Mobile/12_Mobile_Android_Matches.jpg', 'Android matches'],
			['/real-life-friends/Mobile/13_Mobile_Android_Settings.jpg', 'Android settings'],
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
