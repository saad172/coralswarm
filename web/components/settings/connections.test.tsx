// @vitest-environment jsdom
import '@testing-library/jest-dom/vitest';
import {render,screen} from '@testing-library/react';
import {expect,it,vi} from 'vitest';
import type {ReactNode} from 'react';
import Connections from '@/app/(app)/settings/connections/page';
vi.mock('@/components/settings/SettingsShell',()=>({SettingsShell:({children}:{children:ReactNode})=><>{children}</>}));
vi.mock('@/lib/appQueries',()=>({oauthConnectionsQueryOptions:()=>{throw new Error('Settings must not fetch duplicate provider credentials');}}));
it('retains the Connectors link without duplicating provider grants',()=>{
 render(<Connections/>);
 expect(screen.getByRole('link',{name:'Connectors'})).toHaveAttribute('href','/connectors');
 expect(screen.queryByText(/permissions? granted/)).not.toBeInTheDocument();
});
