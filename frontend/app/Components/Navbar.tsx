import Image from "next/image";
import Link from "next/link";

type NavbarAction = {
	label: string;
	href?: string;
};

type NavbarProps = {
	brand?: string;
	actions?: NavbarAction[];
};

const defaultActions: NavbarAction[] = [
	{ label: "Create", href: "/Create" },
    { label: "Get", href: "/Fetch" },
	{ label: "Update", href: "/Update" },
	{ label: "Delete", href: "/Delete" },
	{ label: "View Schema", href: "/Schema" },
	{ label: "Dump/Load", href: "/DumpLoad" },
];

export default function Navbar({
	brand = "IITGnDB",
	actions = defaultActions,
}: NavbarProps) {
	return (
		<header className="sticky top-4 z-20">
			<nav className="flex items-center justify-between gap-4 rounded-2xl border border-slate-200 bg-white/95 px-4 py-3 shadow-[0_14px_36px_-28px_rgba(15,23,42,0.35)] backdrop-blur-sm sm:px-6">
				<Link
					href="/"
					className="inline-flex items-center gap-2 whitespace-nowrap rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold tracking-[0.16em] text-slate-700 uppercase"
				>
					<Image
						src="/next.svg"
						alt="IITGnDB logo"
						width={18}
						height={18}
					/>
					{brand}
				</Link>

				<ul className="flex flex-wrap items-center justify-end gap-2 sm:gap-3">
					{actions.map((action) => (
						<li key={action.label}>
							<Link
								href={action.href ?? "/"}
								className="inline-flex rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 transition-colors duration-200 hover:border-slate-300 hover:bg-slate-50"
							>
								{action.label}
							</Link>
						</li>
					))}
				</ul>
			</nav>
		</header>
	);
}
