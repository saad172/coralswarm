export const usePathname = () => location.pathname; export const useSearchParams = () => new URLSearchParams(location.search); export const useRouter = () => ({replace:()=>{},push:()=>{}});export const redirect = () => {}; export const RedirectType = {push:'push',replace:'replace'};
export const useSelectedLayoutSegments = () => []; export const useParams = () => ({});
